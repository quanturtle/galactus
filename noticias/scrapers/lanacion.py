import json
import logging

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from the_scraper.html_cleaner import compress

from noticias.scrapers._base import ApiScraper

logger = logging.getLogger(__name__)


class LaNacionScraper(ApiScraper):
    """La Nacion scraper — Arc Publishing CMS with open-ended pagination."""

    source = "lanacion"

    def __init__(self, session: AsyncSession):
        super().__init__(session)
        self.website = self.cfg.get("website", "lanacionpy")

    def _build_params(self, page_index: int) -> dict:
        return {
            "query": json.dumps({
                "feedSize": str(self.page_size),
                "feedFrom": str(page_index * self.page_size),
                "website": self.website,
                "feedQuery": "type:story",
            })
        }

    def _extract_total_pages(self, response: httpx.Response) -> int:
        elements = response.json().get("content_elements", [])
        return 2 if len(elements) >= self.page_size else 1

    async def scrape(self) -> None:
        """Open-ended pagination — override scrape entirely."""
        already_fetched = await self.storage.load_today_endpoints(self.source)
        total_stored = 0
        total_skipped = 0
        page = 0

        while not self.max_pages or total_stored < self.max_pages:
            params = self._build_params(page)
            endpoint = self._build_endpoint(params)

            if endpoint in already_fetched:
                total_skipped += 1
                page += 1
                continue

            try:
                resp = await self.fetch(self.api_url, params=params)
            except Exception:
                logger.exception("%s: failed page %d", self.source, page)
                break

            elements = resp.json().get("content_elements", [])
            if not elements:
                break

            await self.storage.store_response(
                self.source, endpoint, params, compress(resp.text),
            )
            total_stored += 1

            if total_stored % 50 == 0:
                await self.storage.flush()

            logger.info(
                "%s: page %d -> %d articles", self.source, page + 1, len(elements),
            )

            if len(elements) < self.page_size:
                break
            page += 1

        await self.storage.flush()
        logger.info(
            "%s: API scrape done — %d stored, %d skipped",
            self.source, total_stored, total_skipped,
        )
