import asyncio
import re
from collections import deque
from pathlib import Path
from typing import ClassVar
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from galactus.config import ExtractOptions
from galactus.core.errors import DatabaseError, HttpError, ScraperError
from galactus.infra.db import Database
from galactus.infra.http import HttpClient, HttpResponse
from galactus.transform.html_parser import compress
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.base import Base

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
SKIP_PREFIXES = ("mailto:", "tel:", "javascript:", "data:", "whatsapp:", "#")

# persist full HTML bodies in bronze.html_snapshots (the parsers need them).
STORE_HTML_BODY = True


class BaseScraper:
    """Template Method base for all scrapers.

    run() owns the BFS over seeds(): fetch a URL, persist it if eligible, then
    expand the frontier with the page's next URLs.

    Public hook surface — override in subclasses, every one ships a default:
        snapshot_model   class var; default HtmlSnapshot. API scrapers set to
                         ApiSnapshot. The discriminator the default
                         build_snapshot() routes on.
        seeds()          initial frontier; default [options.base_url].
        should_persist() gate before building a snapshot; default = scrape-pattern
                         check (allow-all when no patterns).
        build_snapshot() construct the bronze record; default routes on
                         snapshot_model. Override entirely for a custom shape.
        build_url(url)   build a request URL. Default returns it as-is.
                         Paginating scrapers override (possibly with a different
                         signature, e.g. build_url(self, page: int)) and call
                         their own build_url from seeds() / next_urls().
        next_urls()      URLs to add to the frontier after visiting one;
                         default scrapes every <a href> (HTML BFS).

    Private internals — not extension points: _fetch, _should_enqueue,
    _crawl_url, run.
    """

    snapshot_model: ClassVar[type[Base]] = HtmlSnapshot

    def __init__(
        self,
        source: str,
        http: HttpClient,
        db: Database,
        options: ExtractOptions,
        concurrency: int,
    ) -> None:
        self.source = source
        self.http = http
        self.db = db
        self.options = options
        self.concurrency = concurrency
        self._allowed_hosts: frozenset[str] = frozenset(
            {urlparse(self.options.base_url).netloc, *self.options.allowed_hosts}
        )
        self._scrape_patterns: list[re.Pattern] = [
            re.compile(p) for p in self.options.scrape_url_patterns
        ]
        self._ignore_patterns: list[re.Pattern] = [
            re.compile(p) for p in self.options.ignore_url_patterns
        ]

    # hook: frontier seeds — the first URLs to crawl
    def seeds(self) -> list[str]:
        return [self.options.base_url]

    # hook: gate persistence — return True to snapshot this url, False to skip
    def should_persist(self, url: str) -> bool:
        if not self._scrape_patterns:
            return True
        return any(p.search(url) for p in self._scrape_patterns)

    # hook: build a request URL. Default returns it as-is. Concrete scrapers
    # override to build URLs relative to options.base_url (and may take a
    # different signature, e.g. (self, page: int)) — they call their own
    # build_url from inside seeds() / next_urls().
    def build_url(self, url: str) -> str:
        return url

    # hook: construct the bronze record for one response. Default routes on
    # snapshot_model; override entirely for a fully custom record shape.
    def build_snapshot(self, url: str, response: HttpResponse) -> Base:
        model = self.snapshot_model
        if model is HtmlSnapshot:
            return HtmlSnapshot(
                source=self.source,
                source_url=url,
                status_code=response.status_code,
                content_type=response.headers.get("content-type", ""),
                response_headers=dict(response.headers),
                html=compress(response.text) if STORE_HTML_BODY else b"",
                is_diff=False,
            )
        if model is ApiSnapshot:
            return ApiSnapshot(
                source=self.source,
                source_url=url,
                request_url=url,
                request_params={},
                status_code=response.status_code,
                response_headers=dict(response.headers),
                body=compress(response.text),
            )
        raise ScraperError(f"{self.source}: no snapshot builder for {model}")

    # hook: a fetched page -> raw candidate URLs (absolute or relative) to crawl
    # next. No filtering here — build_url canonicalizes and _should_enqueue
    # gates before enqueueing. Default scrapes every <a href>; a JSON body
    # yields none.
    def next_urls(self, url: str, response: HttpResponse) -> list[str]:
        soup = BeautifulSoup(response.text, "html.parser")
        out: list[str] = []
        for a in soup.find_all("a", href=True):
            href = str(a["href"]).strip()
            if not href:
                continue
            out.append(urljoin(url, href))
        return out

    async def _fetch(self, url: str) -> HttpResponse:
        try:
            return await self.http.get(url)
        except HttpError as exc:
            raise ScraperError(f"{self.source}: GET {url} failed") from exc

    def _should_enqueue(self, url: str) -> bool:
        if url.startswith(SKIP_PREFIXES):
            return False
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        if parsed.netloc not in self._allowed_hosts:
            return False
        if Path(parsed.path).suffix.lower() in SKIP_EXTENSIONS:
            return False
        if self._scrape_patterns and not any(p.search(url) for p in self._scrape_patterns):
            return False
        return not any(p.search(url) for p in self._ignore_patterns)

    async def _crawl_url(
        self,
        url: str,
        frontier: deque[str],
        seen: set[str],
        state: dict[str, int],
    ) -> None:
        """Fetch one URL, persist it if eligible, then expand the frontier with the page's next URLs."""
        response = await self._fetch(url)

        # build and persist snapshot if eligible
        if self.should_persist(url):
            record = self.build_snapshot(url, response)
            try:
                await self.db.insert(record, model=type(record))
            except DatabaseError as exc:
                raise ScraperError(f"{self.source}: persisting {url} failed") from exc
            state["fetched"] += 1

        # expand — sync, race-free w.r.t. other tasks; do not introduce awaits here
        for href in self.next_urls(url, response):
            if href in seen or not self._should_enqueue(href):
                continue
            seen.add(href)
            frontier.append(href)

        # per-task pacing: each in-flight task self-throttles after its fetch
        if self.options.request_delay:
            await asyncio.sleep(self.options.request_delay)
        return

    async def run(self) -> None:
        """Lifecycle: BFS over seeds(); fetch up to self.concurrency URLs in parallel."""
        # init frontier
        initial = self.seeds()
        frontier: deque[str] = deque(initial)
        seen: set[str] = set(initial)
        state: dict[str, int] = {"fetched": 0}
        max_pages = self.options.max_pages
        concurrency = self.concurrency
        in_flight: set[asyncio.Task[None]] = set()

        # spawn-and-drain loop: top up to `concurrency` tasks; drain as they finish.
        # max_pages is a soft cap: once spawning stops, up to concurrency-1 in-flight
        # tasks may still persist, so the final count can overshoot by that much.
        while frontier or in_flight:
            while (
                frontier
                and len(in_flight) < concurrency
                and (max_pages == 0 or state["fetched"] < max_pages)
            ):
                url = frontier.popleft()
                in_flight.add(asyncio.create_task(self._crawl_url(url, frontier, seen, state)))

            if not in_flight:
                break

            done, in_flight = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
        return
