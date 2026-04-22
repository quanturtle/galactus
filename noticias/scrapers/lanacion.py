import json
import logging

from the_scraper.html_cleaner import compress, compute_content_hash

from noticias.scrapers._base import BfsScraper

logger = logging.getLogger(__name__)


class LaNacionScraper(BfsScraper):
    """La Nacion scraper — Arc Publishing feed returns metadata only, so we
    discover URLs via the feed API and fetch each article's detail HTML,
    storing cleaned snapshots in bronze.snapshots."""

    source = "lanacion"

    async def scrape(self) -> None:
        api_url = self.cfg["base_url"] + self.cfg["api_endpoint"]
        page_size = self.cfg.get("page_size", 100)
        website = self.cfg.get("website", "lanacionpy")

        total_attempted = 0
        total_day_skipped = 0
        page = 0

        while not self.max_pages or total_attempted < self.max_pages:
            params = {
                "query": json.dumps({
                    "feedSize": str(page_size),
                    "feedFrom": str(page * page_size),
                    "website": website,
                    "feedQuery": "type:story",
                })
            }
            try:
                feed_resp = await self.fetch(api_url, params=params)
            except Exception:
                logger.exception("%s: failed feed page %d", self.source, page)
                break

            elements = feed_resp.json().get("content_elements", [])
            if not elements:
                break

            page_urls: list[str] = []
            for gc in elements:
                canonical = gc.get("canonical_url", "")
                if not canonical:
                    continue
                url = self.cfg["base_url"] + canonical if canonical.startswith("/") else canonical
                page_urls.append(url)

            already_today = await self.storage.load_today_urls(self.source, page_urls)
            total_day_skipped += len(already_today)

            for url in page_urls:
                if self.max_pages and total_attempted >= self.max_pages:
                    break
                if url in already_today:
                    continue

                try:
                    article_resp = await self.fetch(url)
                except Exception:
                    logger.exception("%s: failed article fetch %s", self.source, url)
                    continue

                cleaned = self.html_cleaner.clean(article_resp.text)
                compressed = compress(cleaned)
                await self.storage.store_snapshot(
                    source=self.source,
                    url=url,
                    html_blob=compressed,
                    content_hash=compute_content_hash(cleaned) if self.use_content_hash else None,
                )
                total_attempted += 1

            await self.storage.flush()
            logger.info(
                "%s: feed page %d — attempted %d, day-skipped %d",
                self.source, page, total_attempted, total_day_skipped,
            )

            if len(elements) < page_size:
                break
            page += 1

        logger.info(
            "%s: scrape done — %d inserted, %d hash-skipped, %d already-today-skipped",
            self.source,
            self.storage.inserted, self.storage.hash_skipped, total_day_skipped,
        )
