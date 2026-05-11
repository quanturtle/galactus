import asyncio
import re
from collections import deque
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from galactus.config import ExtractConfig
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

# temporary: skip persisting full HTML bodies while testing scrapers. flip back to True
# to restore real snapshots.
STORE_HTML_BODY = False


def query_int(url: str, name: str, default: int) -> int:
    """Read an int-valued query param out of a URL, falling back to default."""
    return int(parse_qs(urlparse(url).query).get(name, [str(default)])[0])


class BaseScraper:
    """Template Method base for all scrapers.

    run() owns the BFS over seed_urls(): fetch a URL, persist it if eligible,
    then expand the frontier with the page's next URLs. Concrete scrapers
    override at most three hooks — seed_urls(), build_snapshot(), next_urls() —
    plus the bronze_model class var (the only required override); every hook
    ships with a working default. Everything else is private: fetching, URL
    canonicalization, the same-site/pattern enqueue gate, and the persist gate.
    """

    bronze_model: ClassVar[type[Base]]
    conflict_columns: ClassVar[tuple[str, ...]] = ("source", "source_url", "created_at")
    exclude_columns: ClassVar[tuple[str, ...]] = ("bronze_id", "created_at")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not hasattr(cls, "bronze_model"):
            raise ScraperError(f"{cls.__name__} must define class variable 'bronze_model'")

    def __init__(
        self,
        source: str,
        http: HttpClient,
        db: Database,
        config: ExtractConfig,
    ) -> None:
        self.source = source
        self.http = http
        self.db = db
        self.config = config
        self.options = config.options
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
    def seed_urls(self) -> list[str]:
        return [self.options.base_url]

    async def _fetch(self, url: str) -> HttpResponse:
        try:
            return await self.http.get(url)
        except HttpError as exc:
            raise ScraperError(f"{self.source}: GET {url} failed") from exc

    def _should_persist(self, url: str) -> bool:
        if not self._scrape_patterns:
            return True
        return any(p.search(url) for p in self._scrape_patterns)

    # hook: response -> bronze row. Default covers HtmlSnapshot / ApiSnapshot;
    # override (alongside bronze_model) for a custom bronze table.
    def build_snapshot(self, url: str, response: HttpResponse) -> Base:
        if self.bronze_model is HtmlSnapshot:
            return HtmlSnapshot(
                source=self.source,
                source_url=url,
                status_code=response.status_code,
                content_type=response.headers.get("content-type", ""),
                response_headers=dict(response.headers),
                html=compress(response.text) if STORE_HTML_BODY else b"",
                is_diff=False,
            )
        if self.bronze_model is ApiSnapshot:
            return ApiSnapshot(
                source=self.source,
                source_url=url,
                request_url=url,
                request_params={},
                status_code=response.status_code,
                response_headers=dict(response.headers),
                body=compress(response.text),
            )
        raise NotImplementedError(f"No default build_snapshot for {self.bronze_model}")

    # hook: a fetched page -> raw candidate URLs (absolute or relative) to crawl
    # next. No filtering here — the base canonicalizes, dedups, and gates before
    # enqueueing. Default scrapes every <a href>; an empty/JSON body yields none.
    def next_urls(self, url: str, response: HttpResponse) -> list[str]:
        soup = BeautifulSoup(response.text, "html.parser")
        out: list[str] = []
        for a in soup.find_all("a", href=True):
            href = str(a["href"]).strip()
            if not href:
                continue
            out.append(urljoin(url, href))
        return out

    def _canonicalize_url(self, href: str, page_url: str) -> str | None:
        for prefix in SKIP_PREFIXES:
            if href.startswith(prefix):
                return None
        parsed = urlparse(urljoin(page_url, href))
        if parsed.scheme not in ("http", "https"):
            return None
        query = urlencode(
            sorted(parse_qs(parsed.query, keep_blank_values=True).items()),
            doseq=True,
        )
        path = parsed.path.rstrip("/") or "/"
        return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, query, ""))

    def _should_enqueue(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc not in self._allowed_hosts:
            return False
        if Path(parsed.path).suffix.lower() in SKIP_EXTENSIONS:
            return False
        if self._scrape_patterns and not any(p.search(url) for p in self._scrape_patterns):
            return False
        return not any(p.search(url) for p in self._ignore_patterns)

    async def _process_url(
        self,
        url: str,
        frontier: deque[str],
        seen: set[str],
        state: dict[str, int],
    ) -> None:
        """Fetch one URL, persist it if eligible, then expand the frontier with the page's next URLs."""
        response = await self._fetch(url)

        # persist
        if self._should_persist(url):
            record = self.build_snapshot(url, response)
            try:
                await self.db.insert(
                    record,
                    model=self.bronze_model,
                    conflict_columns=self.conflict_columns,
                    exclude_columns=self.exclude_columns,
                )
            except DatabaseError as exc:
                raise ScraperError(f"{self.source}: persisting {url} failed") from exc
            state["fetched"] += 1

        # expand — sync, race-free w.r.t. other tasks; do not introduce awaits here
        for href in self.next_urls(url, response):
            link = self._canonicalize_url(href, url)
            if not link or link in seen or not self._should_enqueue(link):
                continue
            seen.add(link)
            frontier.append(link)

        # per-task pacing: each in-flight task self-throttles after its fetch
        if self.options.request_delay:
            await asyncio.sleep(self.options.request_delay)
        return

    async def run(self) -> None:
        """Lifecycle: BFS over seed_urls(); fetch up to self.config.concurrency URLs in parallel."""
        # init frontier
        initial = self.seed_urls()
        frontier: deque[str] = deque(initial)
        seen: set[str] = set(initial)
        state: dict[str, int] = {"fetched": 0}
        max_pages = self.options.max_pages
        concurrency = self.config.concurrency
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
                in_flight.add(asyncio.create_task(self._process_url(url, frontier, seen, state)))

            if not in_flight:
                break

            done, in_flight = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
        return
