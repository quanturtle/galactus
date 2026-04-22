import asyncio
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
        self._storage_lock = asyncio.Lock()

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

    async def _fetch_and_store(self, endpoint: str, params: dict) -> bool:
        """Fetch one page and hand it to storage. Returns True on success."""
        try:
            resp = await self.fetch(self.api_url, params=params)
        except Exception as e:
            logger.warning("%s: page fetch failed for %s: %s", self.source, endpoint, e)
            return False
        async with self._storage_lock:
            await self.storage.store_response(
                self.source, endpoint, params, compress(resp.text),
            )
        return True

    async def scrape(self) -> None:
        already_fetched = await self.storage.load_today_endpoints(self.source)
        total_stored = 0
        total_skipped = 0
        total_errors = 0

        first_params = self._build_params(0)
        first_endpoint = self._build_endpoint(first_params)
        resp = await self.fetch(self.api_url, params=first_params)
        total_pages = self._extract_total_pages(resp)
        if self.max_pages:
            total_pages = min(total_pages, self.max_pages)

        logger.info("%s: %d total pages", self.source, total_pages)

        if first_endpoint not in already_fetched:
            async with self._storage_lock:
                await self.storage.store_response(
                    self.source, first_endpoint, first_params, compress(resp.text),
                )
            total_stored += 1
        else:
            total_skipped += 1

        pending: list[tuple[str, dict]] = []
        for page_index in range(1, total_pages):
            params = self._build_params(page_index)
            endpoint = self._build_endpoint(params)
            if endpoint in already_fetched:
                total_skipped += 1
                continue
            pending.append((endpoint, params))

        # Concurrency is already bounded by BaseScraper.semaphore inside fetch();
        # as_completed + per-task storage writes keep in-flight responses bounded
        # and flush progressively instead of holding every body until gather resolves.
        tasks = [
            asyncio.create_task(self._fetch_and_store(endpoint, params))
            for endpoint, params in pending
        ]
        for i, coro in enumerate(asyncio.as_completed(tasks), start=1):
            ok = await coro
            if ok:
                total_stored += 1
            else:
                total_errors += 1
            if i % 10 == 0 or i == len(tasks):
                logger.info(
                    "%s: %d / %d pages processed (stored %d, errors %d, skipped %d)",
                    self.source, i, len(tasks), total_stored, total_errors, total_skipped,
                )

        await self.storage.flush()
        logger.info(
            "%s: API scrape done — %d stored, %d errors, %d skipped",
            self.source, total_stored, total_errors, total_skipped,
        )
