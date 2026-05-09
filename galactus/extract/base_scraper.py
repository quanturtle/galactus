import asyncio
import json
import re
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from galactus.config import ExtractConfig
from galactus.core.errors import ScraperError
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


class BaseScraper:
    """Template Method base for all scrapers.

    run() owns the BFS over seed_urls(), fetch(), persist, then expand. Concrete
    scrapers override hooks (bronze_model class var is the only required override);
    every other hook ships with a working default.
    """

    bronze_model: ClassVar[type[Base]]
    conflict_columns: ClassVar[tuple[str, ...]] = ("source", "source_url", "fetched_at")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not hasattr(cls, "bronze_model"):
            raise ScraperError(f"{cls.__name__} must define class variable 'bronze_model'")

    def __init__(
        self,
        source: str,
        http: HttpClient,
        db: Database,
        bronze_table: str,
        config: ExtractConfig,
    ) -> None:
        self.source = source
        self.http = http
        self.db = db
        self.bronze_table = bronze_table
        self.config = config
        self.options = config.options
        self.home_domain: str = urlparse(self.options.base_url).netloc
        self._scrape_patterns: list[re.Pattern] = [
            re.compile(p) for p in self.options.scrape_url_patterns
        ]
        self._ignore_patterns: list[re.Pattern] = [
            re.compile(p) for p in self.options.ignore_url_patterns
        ]

    # 1. seed_urls — frontier init
    def seed_urls(self) -> list[str]:
        return [self.options.base_url]

    # 2. fetch — network -> HttpResponse
    async def fetch(self, url: str) -> HttpResponse:
        return await self.http.get(url)

    # 3. should_persist — gate: keep this response?
    def should_persist(self, url: str, response: HttpResponse) -> bool:
        if not self._scrape_patterns:
            return True
        return any(p.search(url) for p in self._scrape_patterns)

    # 4. build_snapshot — response -> bronze row
    def build_snapshot(self, url: str, response: HttpResponse) -> Base:
        now = datetime.now(tz=UTC)
        if self.bronze_model is HtmlSnapshot:
            return HtmlSnapshot(
                source=self.source,
                source_url=url,
                fetched_at=now,
                status_code=response.status_code,
                content_type=response.headers.get("content-type", ""),
                response_headers=dict(response.headers),
                html=compress(response.text),
                is_diff=False,
            )
        if self.bronze_model is ApiSnapshot:
            return ApiSnapshot(
                source=self.source,
                source_url=url,
                fetched_at=now,
                request_url=url,
                request_params={},
                status_code=response.status_code,
                response_headers=dict(response.headers),
                body=compress(response.text),
            )
        raise NotImplementedError(f"No default build_snapshot for {self.bronze_model}")

    # 5a. _walk_for_urls — recursive helper: collect URL-like strings from a dict/list
    def _walk_for_urls(self, value: Any) -> list[str]:
        out: list[str] = []
        if isinstance(value, str):
            if value.startswith(("http://", "https://", "/")):
                out.append(value)
        elif isinstance(value, dict):
            for v in value.values():
                out.extend(self._walk_for_urls(v))
        elif isinstance(value, list):
            for v in value:
                out.extend(self._walk_for_urls(v))
        return out

    # 5a'. _is_same_domain — netloc match against home_domain, tolerant of www. prefix
    def _is_same_domain(self, absolute_url: str) -> bool:
        netloc = urlparse(absolute_url).netloc
        if not netloc:
            return False
        if not netloc.startswith("www."):
            netloc = "www." + netloc
        return netloc == self.home_domain

    # 5a''. _is_followable — same-domain and not a blocked file extension
    def _is_followable(self, absolute_url: str) -> bool:
        if not self._is_same_domain(absolute_url):
            return False
        ext = Path(urlparse(absolute_url).path).suffix.lower()
        return ext not in SKIP_EXTENSIONS

    # 5b. discover_json_links — JSON case
    def discover_json_links(self, url: str, response: HttpResponse) -> list[str]:
        try:
            payload = response.json()
        except json.JSONDecodeError:
            return []
        out: list[str] = []
        for href in self._walk_for_urls(payload):
            absolute = urljoin(url, href)
            if self._is_followable(absolute):
                out.append(absolute)
        return out

    # 5c. discover_html_links — HTML case
    def discover_html_links(self, url: str, response: HttpResponse) -> list[str]:
        soup = BeautifulSoup(response.text, "html.parser")
        out: list[str] = []
        for a in soup.find_all("a", href=True):
            href = str(a["href"]).strip()
            if not href:
                continue
            absolute = urljoin(url, href)
            if self._is_followable(absolute):
                out.append(absolute)
        return out

    # 5. discover_links — dispatcher: response -> raw hrefs
    def discover_links(self, url: str, response: HttpResponse) -> list[str]:
        content_type = response.headers.get("content-type", "").lower()
        if "json" in content_type:
            return self.discover_json_links(url, response)
        return self.discover_html_links(url, response)

    # 6. normalize_url — href -> absolute URL or None
    def normalize_url(self, href: str, page_url: str) -> str | None:
        for prefix in SKIP_PREFIXES:
            if href.startswith(prefix):
                return None
        absolute = urljoin(page_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            return None
        scheme = "https"
        netloc = parsed.netloc if parsed.netloc.startswith("www.") else "www." + parsed.netloc
        query = urlencode(
            sorted(parse_qs(parsed.query, keep_blank_values=True).items()),
            doseq=True,
        )
        path = parsed.path.rstrip("/") or "/"
        return urlunparse((scheme, netloc, path, parsed.params, query, ""))

    # 7. should_enqueue — gate: add to frontier?
    def should_enqueue(self, url: str) -> bool:
        if urlparse(url).netloc != self.home_domain:
            return False
        ext = Path(urlparse(url).path).suffix.lower()
        if ext in SKIP_EXTENSIONS:
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
        """Fetch one URL, persist if eligible, then expand frontier with discovered links."""
        response = await self.fetch(url)

        # persist
        if self.should_persist(url, response):
            record = self.build_snapshot(url, response)
            await self.db.insert(
                record, model=self.bronze_model, conflict_columns=self.conflict_columns
            )
            state["fetched"] += 1

        # sync — race-free w.r.t. other tasks; do not introduce awaits
        for href in self.discover_links(url, response):
            link = self.normalize_url(href, url)
            if not link or link in seen or not self.should_enqueue(link):
                continue
            seen.add(link)
            frontier.append(link)

        # per-task pacing: each in-flight task self-throttles after its fetch
        if self.options.request_delay:
            await asyncio.sleep(self.options.request_delay)
        return

    async def run(self) -> None:
        """Lifecycle: BFS over seed_urls(); fetch up to self.http.concurrency URLs in parallel."""
        # init frontier
        initial = self.seed_urls()
        frontier: deque[str] = deque(initial)
        seen: set[str] = set(initial)
        state: dict[str, int] = {"fetched": 0}
        max_pages = self.options.max_pages
        concurrency = self.http.concurrency
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
