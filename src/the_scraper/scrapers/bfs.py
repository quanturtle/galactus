import asyncio
import logging
import re
from collections import deque
from pathlib import Path
from urllib.parse import urlparse

from the_scraper.html_cleaner import HtmlCleaner, compress, compute_content_hash
from the_scraper.storage import SnapshotStorage
from the_scraper.urls import extract_same_domain_links, normalize, should_ignore

from .base import BaseScraper

logger = logging.getLogger(__name__)

DEFAULT_BFS_MAX_PAGES = 5000
DEFAULT_BFS_BATCH_SIZE = 20
DEFAULT_BFS_REQUEST_DELAY = 0.0


class BfsScraper(BaseScraper):
    """BFS crawler that discovers pages and stores cleaned HTML snapshots.

    Visits every same-domain page reachable from home_url. For pages
    matching scrape_pattern, cleans + compresses the HTML and stores
    it via the SnapshotStorage protocol.
    """

    def __init__(
        self,
        *,
        storage: SnapshotStorage,
        config_dir: Path | str,
        html_cleaner: HtmlCleaner | None = None,
        use_content_hash: bool = False,
        batch_size: int | None = None,
        **kwargs,
    ):
        super().__init__(config_dir=config_dir, **kwargs)
        self.storage = storage
        self.html_cleaner = html_cleaner or HtmlCleaner()
        self.use_content_hash = use_content_hash

        self.home_url = self.cfg["home_url"]
        self.scrape_pattern = self.cfg["scrape_pattern"]
        self.max_pages = self.cfg.get("max_pages", DEFAULT_BFS_MAX_PAGES)
        self.batch_size = batch_size or self.cfg.get("batch_size", DEFAULT_BFS_BATCH_SIZE)
        self.request_delay = self.cfg.get("request_delay", DEFAULT_BFS_REQUEST_DELAY)
        self.ignore_patterns = self.cfg.get("ignore_patterns", [])
        self.strip_path_prefixes = self.cfg.get("strip_path_prefixes")

        # Per-source extras layered on top of the framework's basic filter
        self.html_cleaner.extra_strip_tags = set(self.cfg.get("strip_tags", []))
        self.html_cleaner.extra_strip_classes = list(self.cfg.get("strip_classes", []))

    async def _process_url(
        self, url: str, target_re: re.Pattern, home_domain: str,
    ) -> tuple[list[str], dict | None]:
        resp = await self.fetch(url)
        raw_html = resp.text

        links = extract_same_domain_links(raw_html, url, home_domain)

        snapshot = None
        if target_re.search(url):
            cleaned = self.html_cleaner.clean(raw_html)
            compressed = compress(cleaned)
            snapshot = {
                "source": self.source,
                "url": url,
                "html_blob": compressed,
            }
            if self.use_content_hash:
                snapshot["content_hash"] = compute_content_hash(cleaned)

        return links, snapshot

    async def scrape(self) -> None:
        seed = normalize(self.home_url)

        target_queue: deque[str] = deque()
        discovery_queue: deque[str] = deque([seed])
        seen: set[str] = {seed}
        visited: set[str] = set()
        total_day_skipped = 0
        total_errors = 0

        target_re = re.compile(self.scrape_pattern)
        ignore_res = [re.compile(p) for p in self.ignore_patterns]
        home_domain = urlparse(self.home_url).netloc

        while (target_queue or discovery_queue) and len(visited) < self.max_pages:
            batch_urls: list[str] = []
            for q in (target_queue, discovery_queue):
                while q and len(batch_urls) < self.batch_size:
                    url = q.popleft()
                    visited.add(url)
                    batch_urls.append(url)

            if not batch_urls:
                break

            already_today = await self.storage.load_today_urls(self.source, batch_urls)
            if already_today:
                total_day_skipped += len(already_today)
                batch_urls = [u for u in batch_urls if u not in already_today]
            if not batch_urls:
                continue

            results = await asyncio.gather(
                *(self._process_url(u, target_re, home_domain) for u in batch_urls),
                return_exceptions=True,
            )

            for url, result in zip(batch_urls, results):
                if isinstance(result, Exception):
                    total_errors += 1
                    continue

                new_links, snapshot = result

                for link in new_links:
                    normalized = normalize(link, self.strip_path_prefixes)
                    if normalized not in seen and not should_ignore(normalized, ignore_res):
                        seen.add(normalized)
                        if target_re.search(normalized):
                            target_queue.append(normalized)
                        else:
                            discovery_queue.append(normalized)

                if snapshot is not None:
                    await self.storage.store_snapshot(**snapshot)

            await self.storage.flush()

            if self.request_delay > 0:
                await asyncio.sleep(self.request_delay)

            queued = len(target_queue) + len(discovery_queue)
            logger.info(
                "%s: visited %d, queued %d (target: %d), errors %d",
                self.source, len(visited), queued, len(target_queue), total_errors,
            )

        logger.info(
            "%s: BFS done — %d visited, %d stored, %d hash-skipped, "
            "%d already-today-skipped, %d errors",
            self.source, len(visited),
            self.storage.inserted, self.storage.hash_skipped,
            total_day_skipped, total_errors,
        )
