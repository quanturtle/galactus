import json
import logging

from the_scraper.html_cleaner import compress

from noticias.scrapers._base import ApiScraper

logger = logging.getLogger(__name__)


class ABCColorScraper(ApiScraper):
    """ABC Color scraper — Arc Publishing CMS with per-section pagination."""

    source = "abc"

    def __init__(self):
        super().__init__()
        self.sections = self.cfg.get("sections", [])

    def _build_params(self, page_index: int) -> dict:
        raise NotImplementedError

    def _extract_total_pages(self, response) -> int:
        raise NotImplementedError

    async def scrape(self) -> None:
        """Per-section pagination — override scrape entirely."""
        already_fetched = await self.storage.load_today_endpoints(self.source)
        total_stored = 0
        total_skipped = 0

        for section in self.sections:
            if self.max_pages and total_stored >= self.max_pages:
                break
            offset = 0
            while True:
                if self.max_pages and total_stored >= self.max_pages:
                    break
                params = {
                    "query": json.dumps({
                        "section_id": section,
                        "sort": "display_date:desc",
                        "limit": str(self.page_size),
                        "offset": str(offset),
                    })
                }
                endpoint = self._build_endpoint(params)

                if endpoint in already_fetched:
                    total_skipped += 1
                    offset += self.page_size
                    break

                try:
                    resp = await self.fetch(self.api_url, params=params)
                except Exception:
                    logger.exception("%s: failed %s offset %d", self.source, section, offset)
                    break

                elements = resp.json().get("content_elements", [])

                await self.storage.store_response(
                    self.source, endpoint, params, compress(resp.text),
                )
                total_stored += 1

                if total_stored % 50 == 0:
                    await self.storage.flush()

                logger.info(
                    "%s: %s offset %d -> %d articles",
                    self.source, section, offset, len(elements),
                )

                if not elements or len(elements) < self.page_size:
                    break
                offset += self.page_size

        await self.storage.flush()
        logger.info(
            "%s: API scrape done — %d stored, %d skipped",
            self.source, total_stored, total_skipped,
        )
