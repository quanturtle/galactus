import logging
from abc import abstractmethod
from pathlib import Path
from urllib.parse import urlencode

import httpx

from the_scraper.html_cleaner import compress
from the_scraper.storage import ApiStorage

from .base import BaseScraper

logger = logging.getLogger(__name__)


class ApiScraper(BaseScraper):
    """Base for API-based scrapers with paginated fetching and compressed storage."""

    def __init__(self, *, storage: ApiStorage, config_dir: Path | str, **kwargs):
        super().__init__(config_dir=config_dir, **kwargs)
        self.storage = storage
        self.api_url = self.cfg["base_url"] + self.cfg["api_endpoint"]
        self.page_size = self.cfg.get("page_size", 100)
        self.max_pages = self.cfg.get("max_pages", 0)

    @abstractmethod
    def _build_params(self, page_index: int) -> dict:
        """Return query params for the given page (0-indexed)."""
        ...

    @abstractmethod
    def _extract_total_pages(self, response: httpx.Response) -> int:
        """Extract total number of pages from the first API response."""
        ...

    def _build_endpoint(self, params: dict) -> str:
        """Build canonical endpoint string (sorted params) for dedup."""
        sorted_params = urlencode(sorted(params.items()))
        return f"{self.api_url}?{sorted_params}" if sorted_params else self.api_url

    async def scrape(self) -> None:
        already_fetched = await self.storage.load_today_endpoints(self.source)
        total_stored = 0
        total_skipped = 0

        first_params = self._build_params(0)
        first_endpoint = self._build_endpoint(first_params)
        resp = await self.fetch(self.api_url, params=first_params)
        total_pages = self._extract_total_pages(resp)
        if self.max_pages:
            total_pages = min(total_pages, self.max_pages)

        logger.info("%s: %d total pages", self.source, total_pages)

        if first_endpoint not in already_fetched:
            await self.storage.store_response(
                self.source, first_endpoint, first_params, compress(resp.text),
            )
            total_stored += 1
        else:
            total_skipped += 1

        for page_index in range(1, total_pages):
            params = self._build_params(page_index)
            endpoint = self._build_endpoint(params)

            if endpoint in already_fetched:
                total_skipped += 1
                continue

            resp = await self.fetch(self.api_url, params=params)
            await self.storage.store_response(
                self.source, endpoint, params, compress(resp.text),
            )
            total_stored += 1

            if total_stored % 50 == 0:
                await self.storage.flush()

            logger.info(
                "%s: fetched page %d / %d, skipped %d",
                self.source, page_index + 1, total_pages, total_skipped,
            )

        await self.storage.flush()
        logger.info(
            "%s: API scrape done — %d pages stored, %d skipped",
            self.source, total_stored, total_skipped,
        )
