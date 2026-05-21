import asyncio
import logging
from collections import deque
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlparse, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from galactus.config import ExtractConfig
from galactus.core.errors import DatabaseError, HttpError, ScraperError
from galactus.extract.html_processor import HtmlProcessor
from galactus.infra.db import Database
from galactus.infra.http import HttpClient, HttpRequest, HttpResponse
from sql.a_bronze.failed_snapshots import FailedSnapshot
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.base import Base

logger = logging.getLogger(__name__)

SKIP_EXTENSIONS = frozenset(
    {
        ".pdf",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".svg",
        ".webp",
        ".css",
        ".js",
        ".zip",
        ".mp4",
        ".mp3",
        ".ico",
        ".woff",
        ".woff2",
        ".rss",
        ".xml",
        ".atom",
    }
)

# tracking params stripped on every build_url so utm-tagged dupes collapse in bronze.
TRACKING_PARAMS = frozenset(
    {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_content",
        "utm_term",
        "utm_id",
        "utm_name",
        "utm_reader",
        "fbclid",
        "gclid",
        "mc_eid",
        "mc_cid",
        "yclid",
        "msclkid",
        "_ga",
        "_gl",
    }
)


class BaseScraper:
    """Template Method base for all scrapers.

    run() opens the HTTP client and DB for this run, then drives a BFS over
    seed_urls(): fetch, persist if eligible, expand the frontier with each
    page's next URLs. Subclasses override the hooks below; every hook ships
    a usable default. Per-site transport tweaks (legacy ciphers, custom DB
    engine kwargs) live on the subclass via http_extras() / db_extras().
    """

    bronze_model: ClassVar[type[Base]] = HtmlSnapshot

    def __init__(self, config: ExtractConfig) -> None:
        self.config = config
        self.source = config.source
        self.concurrency = config.concurrency
        self.request_delay = config.request_delay
        self.html_processor: HtmlProcessor | None = self.make_html_processor()
        # populated in run(), inside the async with
        self.http: HttpClient
        self.db: Database
        # field order mirrors source yaml
        logger.info(
            "Scraper initialized (source=%s, scraper=%s, base_url=%s, max_pages=%s, "
            "concurrency=%s, timeout_seconds=%s, request_delay=%s)",
            self.source,
            type(self).__name__,
            config.base_url,
            config.max_pages,
            config.concurrency,
            config.timeout_seconds,
            config.request_delay,
        )

    def http_extras(self) -> dict[str, Any]:
        return {}

    def db_extras(self) -> dict[str, Any]:
        return {}

    def make_http_client(self) -> HttpClient:
        return HttpClient(
            timeout=self.config.timeout_seconds,
            follow_redirects=self.config.follow_redirects,
            pool_size=self.config.concurrency,
            **self.http_extras(),
        )

    def make_database(self) -> Database:
        return Database(
            database_url=self.config.database_url,
            pool_size=self.config.db_pool_size,
            **self.db_extras(),
        )

    def make_html_processor(self) -> HtmlProcessor | None:
        # API snapshots persist the raw body; only HTML snapshots need cleaning passes.
        if self.bronze_model is not HtmlSnapshot:
            return None
        return HtmlProcessor(
            {
                "blocklist_tags": self.config.blocklist_tags,
                "blocklist_attributes": self.config.blocklist_attributes,
            }
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(self.config.base_url)]

    async def fetch(self, request: HttpRequest) -> HttpResponse:
        # per-fetch politeness pause; runs in each fetch task so it overlaps under concurrency.
        # exceptions propagate so the run loop can skip the failed URL instead of aborting the crawl.
        if self.request_delay:
            await asyncio.sleep(self.request_delay)
        return await self.http.get(request)

    # JSON bodies yield no matching tags, so API subclasses inherit a no-op default
    # and ignore `soup` (None on non-HTML responses).
    def extract_links(self, response: HttpResponse, soup: BeautifulSoup | None) -> list[str]:
        if soup is None:
            return []
        # bare path-relative hrefs (`catalogo/foo`) resolve against base_url so they don't
        # stack onto response.url's path. Query- and fragment-only hrefs (`?p=3`, `#x`)
        # keep response.url per RFC 3986 so pagination anchors like `<a href="?p=3">` work.
        root = self.config.base_url.rstrip("/") + "/"
        out: list[str] = []
        tags = [*soup.select("a[href]"), *soup.find_all("link", rel="next", href=True)]
        for tag in tags:
            href = str(tag["href"]).strip()
            if not href:
                continue
            base = response.url if urlparse(href).scheme or href.startswith(("/", "?", "#")) else root
            out.append(urljoin(base, href))
        return out

    # canonicalize: lowercase scheme+host, strip TRACKING_PARAMS, drop fragment, keep %20 (quote, not quote_plus).
    # path case and trailing slash are preserved — source sites link consistently within themselves.
    def build_url(
        self,
        url: str,
        params: dict[str, str] | None = None,
    ) -> HttpRequest:
        parts = urlsplit(url)
        kept = [
            (k, v)
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k not in TRACKING_PARAMS
        ]
        canonical = urlunsplit(
            (
                parts.scheme.lower(),
                parts.netloc.lower(),
                parts.path,
                urlencode(kept, quote_via=quote),
                "",
            )
        )
        return HttpRequest(
            url=canonical,
            headers=dict(self.config.headers),
            params=dict(params or {}),
        )

    def get_next_urls(
        self, response: HttpResponse, soup: BeautifulSoup | None = None
    ) -> list[HttpRequest]:
        return [self.build_url(link) for link in self.extract_links(response, soup)]

    # empty netloc on mailto:/tel:/javascript: makes them fall through to False.
    def should_enqueue(self, request: HttpRequest) -> bool:
        parsed = urlparse(request.url)
        if parsed.netloc not in self.config.allowed_domains:
            return False
        if Path(parsed.path).suffix.lower() in SKIP_EXTENSIONS:
            return False
        # reject paths with a repeated segment — symptom of bad relative-link resolution.
        segments = [s for s in parsed.path.split("/") if s]
        if len(segments) != len(set(segments)):
            return False
        return not any(p.search(request.url) for p in self.config.ignore_patterns)

    def should_persist(self, request: HttpRequest) -> bool:
        if not self.config.scrape_patterns:
            return True
        return any(p.search(request.url) for p in self.config.scrape_patterns)

    async def seen_today(self) -> set[int]:
        """Hashes of requests this source already captured (2xx) since UTC midnight.

        Pre-loaded into the BFS `seen` set so a same-day rerun re-fetches the
        seeds (to discover new content) but skips any request already in bronze.
        Each hash is produced via build_url so the keys match what BFS expansion
        produces for in-flight requests. Paginating subclasses override build_url
        with non-URL signatures, so we go through the keyword path (url=, params=)
        that every build_url accepts; the stored request_url is already canonical
        and request_params carries the per-page differentiator when paginated.
        """
        rows = await self.db.load_visited_requests(
            model=self.bronze_model,
            source=self.source,
        )
        return {hash(self.build_url(url=url, params=params)) for url, params in rows}

    async def extract_body(self, response: HttpResponse, soup: BeautifulSoup | None) -> str:
        # JSON path (no soup parsed): persist the raw response text.
        # HTML path: emit the cleaned soup tree.
        if soup is None:
            return response.text
        return await self.html_processor.clean(soup)

    def is_success(self, response: HttpResponse) -> bool:
        return 200 <= response.status_code < 300

    def snapshot_model(self, response: HttpResponse) -> type[Base] | None:
        """The table this response belongs in: the bronze model for a successful
        request worth persisting, FailedSnapshot for a failed one, None to discard."""
        if not self.should_persist(response.request):
            return None
        return self.bronze_model if self.is_success(response) else FailedSnapshot

    async def store_snapshot(self, response: HttpResponse, model: type[Base], body: str) -> None:
        """Build a snapshot row (same columns for every table) and insert it."""
        request = response.request
        record = model(
            source=self.source,
            request_url=request.url,
            request_headers=dict(request.headers),
            request_params=request.params or {},
            status_code=response.status_code,
            content_type=response.headers.get("content-type", ""),
            response_headers=dict(response.headers),
            body=self.db.compress(body),
        )
        try:
            await self.db.insert(record, model=model)
        except DatabaseError as exc:
            raise ScraperError(f"{self.source}: persisting {request.url} failed") from exc
        logger.info(
            "extract[%s]: persisted %s for %s",
            self.source,
            model.__name__,
            request.url,
        )
        return

    async def process_response(self, response: HttpResponse) -> list[HttpRequest]:
        model = self.snapshot_model(response)

        # an error response is recorded as-is — never parsed, never expanded
        if not self.is_success(response):
            if model is not None:
                await self.store_snapshot(response, model, response.text)
            return []

        # a successful response is parsed, persisted if eligible, then expanded.
        # API subclasses set bronze_model = ApiSnapshot, so html_processor is None.
        soup = self.html_processor.parse(response.text) if self.html_processor else None
        if model is not None:
            await self.store_snapshot(response, model, await self.extract_body(response, soup))
        return self.get_next_urls(response, soup)

    async def run(self) -> None:
        """Lifecycle: open clients; BFS over seed_urls(); fetch up to self.concurrency in parallel."""
        async with self.make_http_client() as http, self.make_database() as db:
            self.http = http
            self.db = db

            seen: set[int] = await self.seen_today()
            already_seen_today = len(seen)
            frontier: deque[HttpRequest] = deque()
            for seed in self.seed_urls():
                key = hash(seed)
                if key in seen:
                    continue
                frontier.append(seed)
                seen.add(key)
            logger.info(
                "extract[%s]: scraper run start (seed_count=%s, already_seen_today=%s)",
                self.source,
                len(frontier),
                already_seen_today,
            )
            dispatched = 0
            skipped = 0
            max_pages = self.config.max_pages
            concurrency = self.concurrency
            in_flight: set[asyncio.Task[HttpResponse]] = set()

            # spawn-and-drain: top up to `concurrency` fetches, then drain on FIRST_COMPLETED.
            # max_pages is a hard cap on dispatched fetches — counted at spawn time so no extras slip through. -1 disables the cap.
            try:
                while frontier or in_flight:
                    while (
                        frontier
                        and len(in_flight) < concurrency
                        and (max_pages == -1 or dispatched < max_pages)
                    ):
                        request = frontier.popleft()
                        in_flight.add(asyncio.create_task(self.fetch(request)))
                        dispatched += 1

                    if not in_flight:
                        break

                    done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        in_flight.discard(task)
                        # per-URL isolation: fetch, persist, and next-url derivation can each fail
                        # for one URL without aborting the source. cancellation still propagates.
                        try:
                            response = await task
                            next_requests = await self.process_response(response)
                        except (HttpError, DatabaseError, ScraperError) as exc:
                            skipped += 1
                            logger.warning(
                                "extract[%s]: skipping after error: %s", self.source, exc
                            )
                            continue
                        for next_request in next_requests:
                            key = hash(next_request)
                            if key in seen or not self.should_enqueue(next_request):
                                continue
                            seen.add(key)
                            frontier.append(next_request)
            finally:
                # drain remaining fetches so a mid-run raise doesn't leak tasks to the loop
                for task in in_flight:
                    task.cancel()
                if in_flight:
                    await asyncio.gather(*in_flight, return_exceptions=True)
        logger.info(
            "extract[%s]: scraper run complete (dispatched=%s, skipped=%s)",
            self.source,
            dispatched,
            skipped,
        )
        return
