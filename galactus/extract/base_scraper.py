import asyncio
import logging
from collections import deque
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from galactus.config import ExtractConfig
from galactus.core.errors import DatabaseError, ScraperError
from galactus.infra.db import Database
from galactus.infra.http import HttpClient, HttpRequest, HttpResponse
from galactus.transform.html_parser import compress
from sql.a_bronze.api_snapshots import ApiSnapshot
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

    snapshot_model: ClassVar[type[Base]] = HtmlSnapshot

    def __init__(self, config: ExtractConfig) -> None:
        self.config = config
        self.source = config.source
        self.concurrency = config.concurrency
        # populated in run(), inside the async with
        self.http: HttpClient
        self.db: Database
        # field order mirrors source yaml
        logger.info(
            "Scraper initialized (source=%s, scraper=%s, base_url=%s, max_pages=%s, "
            "concurrency=%s, timeout_seconds=%s, retries=%s, retry_delay=%s, request_delay=%s)",
            self.source, type(self).__name__, config.base_url, config.max_pages,
            config.concurrency, config.timeout_seconds, config.retries, config.retry_delay,
            config.request_delay,
        )

    def http_extras(self) -> dict[str, Any]:
        return {}

    def db_extras(self) -> dict[str, Any]:
        return {}

    def make_http_client(self) -> HttpClient:
        return HttpClient(
            timeout=self.config.timeout_seconds,
            retries=self.config.retries,
            retry_delay=self.config.retry_delay,
            pool_size=self.config.concurrency,
            **self.http_extras(),
        )

    def make_database(self) -> Database:
        return Database(
            database_url=self.config.database_url,
            pool_size=self.config.db_pool_size,
            **self.db_extras(),
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(self.config.base_url)]

    async def fetch(self, request: HttpRequest) -> HttpResponse:
        # exceptions propagate so the run loop can skip the failed URL instead of aborting the crawl.
        return await self.http.get(request)

    # JSON bodies yield no <a href>, so API subclasses inherit a no-op default.
    def extract_links(self, response: HttpResponse) -> list[str]:
        soup = BeautifulSoup(response.text, "html.parser")
        out: list[str] = []
        for a in soup.find_all("a", href=True):
            href = str(a["href"]).strip()
            if not href:
                continue
            out.append(urljoin(response.url, href))
        return out

    # single seam: canonicalize URL (strip tracking, lowercase scheme+host) and attach config headers/params.
    # kwargs branch (url=, params=) is the seen_today path — bronze is already canonical, don't re-canonicalize.
    def build_url(self, *args: Any, **kwargs: Any) -> HttpRequest:
        if "url" in kwargs:
            return HttpRequest(
                url=kwargs["url"],
                headers=dict(self.config.headers),
                params=dict(kwargs.get("params") or {}),
            )
        parts = urlsplit(args[0])
        kept = [
            (k, v)
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k not in TRACKING_PARAMS
        ]
        url = urlunsplit(
            (
                parts.scheme.lower(),
                parts.netloc.lower(),
                parts.path,
                urlencode(kept),
                parts.fragment,
            )
        )
        return HttpRequest(
            url=url,
            headers=dict(self.config.headers),
            params=dict(self.config.params),
        )

    def get_next_urls(self, response: HttpResponse) -> list[HttpRequest]:
        return [self.build_url(link) for link in self.extract_links(response)]

    # empty netloc on mailto:/tel:/javascript: makes them fall through to False.
    def should_enqueue(self, request: HttpRequest) -> bool:
        parsed = urlparse(request.url)
        if parsed.netloc not in self.config.allowed_domains:
            return False
        if Path(parsed.path).suffix.lower() in SKIP_EXTENSIONS:
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
        that every build_url accepts; the stored source_url is already canonical
        and request_params (when present) carries the per-page differentiator.
        """
        rows = await self.db.load_visited_requests(
            model=self.snapshot_model,
            source=self.source,
        )
        return {hash(self.build_url(url=url, params=params)) for url, params in rows}

    async def process_response(self, response: HttpResponse) -> list[HttpRequest]:
        # dispatch on snapshot_model — subclasses set ApiSnapshot to swap the bronze record shape.
        request = response.request
        if self.should_persist(request):
            model = self.snapshot_model
            if model is HtmlSnapshot:
                record: Base = HtmlSnapshot(
                    source=self.source,
                    source_url=request.url,
                    status_code=response.status_code,
                    content_type=response.headers.get("content-type", ""),
                    response_headers=dict(response.headers),
                    html=compress(response.text),
                    is_diff=False,
                )
            elif model is ApiSnapshot:
                record = ApiSnapshot(
                    source=self.source,
                    source_url=request.url,
                    request_url=request.url,
                    request_params=request.params,
                    status_code=response.status_code,
                    response_headers=dict(response.headers),
                    body=compress(response.text),
                )
            else:
                raise ScraperError(f"{self.source}: no snapshot builder for {model}")

            try:
                await self.db.insert(record, model=type(record))
            except DatabaseError as exc:
                raise ScraperError(f"{self.source}: persisting {request.url} failed") from exc
            logger.info(
                "extract[%s]: persisted %s for %s",
                self.source, type(record).__name__, request.url,
            )

        return self.get_next_urls(response)

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
                self.source, len(frontier), already_seen_today,
            )
            dispatched = 0
            skipped = 0
            max_pages = self.config.max_pages
            in_flight: set[asyncio.Task[HttpResponse]] = set()

            # spawn-and-drain: top up to `concurrency` fetches, then drain on FIRST_COMPLETED.
            # max_pages is a hard cap on dispatched fetches — counted at spawn time so no extras slip through. -1 disables the cap.
            try:
                while frontier or in_flight:
                    while (
                        frontier
                        and len(in_flight) < self.concurrency
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
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            skipped += 1
                            logger.warning("extract[%s]: skipping after error: %s", self.source, exc)
                            continue
                        for next_request in next_requests:
                            key = hash(next_request)
                            if key in seen or not self.should_enqueue(next_request):
                                continue
                            seen.add(key)
                            frontier.append(next_request)
                        if self.config.request_delay:
                            await asyncio.sleep(self.config.request_delay)
            finally:
                # drain remaining fetches so a mid-run raise doesn't leak tasks to the loop
                for task in in_flight:
                    task.cancel()
                if in_flight:
                    await asyncio.gather(*in_flight, return_exceptions=True)
        logger.info(
            "extract[%s]: scraper run complete (dispatched=%s, skipped=%s)",
            self.source, dispatched, skipped,
        )
        return
