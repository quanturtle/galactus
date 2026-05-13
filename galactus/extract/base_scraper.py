import asyncio
import re
from collections import deque
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from galactus.config import ExtractConfig, ExtractOptions
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

# persist full HTML bodies in bronze.html_snapshots (the parsers need them).
STORE_HTML_BODY = True


class BaseScraper:
    """Template Method base for all scrapers.

    run() drives a BFS over seed_urls(): fetch, persist if eligible, expand
    the frontier with each page's next URLs. Subclasses override the hooks
    below; every hook ships a usable default.
    """

    snapshot_model: ClassVar[type[Base]] = HtmlSnapshot
    # extra kwargs forwarded to HttpClient at instantiation — sources that need
    # site-specific transport tweaks (legacy ciphers, custom verify, ...) override.
    http_kwargs: ClassVar[dict[str, Any]] = {}

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

    def seed_urls(self) -> list[str]:
        return [self.options.base_url]

    async def fetch(self, url: str) -> HttpResponse:
        try:
            return await self.http.get(url)
        except HttpError as exc:
            raise ScraperError(f"{self.source}: GET {url} failed") from exc

    # JSON bodies yield no <a href>, so API subclasses inherit a no-op default.
    def extract_links(self, url: str, response: HttpResponse) -> list[str]:
        soup = BeautifulSoup(response.text, "html.parser")
        out: list[str] = []
        for a in soup.find_all("a", href=True):
            href = str(a["href"]).strip()
            if not href:
                continue
            out.append(urljoin(url, href))
        return out

    # *args/**kwargs lets paginating subclasses reshape the signature (e.g. build_url(page)).
    def build_url(self, *args, **kwargs) -> str:
        return args[0]

    def get_next_urls(self, url: str, response: HttpResponse) -> list[str]:
        return [self.build_url(link) for link in self.extract_links(url, response)]

    # empty netloc on mailto:/tel:/javascript: makes them fall through to False.
    def should_enqueue(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc not in self._allowed_hosts:
            return False
        if Path(parsed.path).suffix.lower() in SKIP_EXTENSIONS:
            return False
        return not any(p.search(url) for p in self._ignore_patterns)

    def should_persist(self, url: str) -> bool:
        if not self._scrape_patterns:
            return True
        return any(p.search(url) for p in self._scrape_patterns)

    async def process_response(self, url: str, response: HttpResponse) -> list[str]:
        # dispatch on snapshot_model — subclasses set ApiSnapshot to swap the bronze record shape.
        if self.should_persist(url):
            model = self.snapshot_model
            if model is HtmlSnapshot:
                record: Base = HtmlSnapshot(
                    source=self.source,
                    source_url=url,
                    status_code=response.status_code,
                    content_type=response.headers.get("content-type", ""),
                    response_headers=dict(response.headers),
                    html=compress(response.text) if STORE_HTML_BODY else b"",
                    is_diff=False,
                )
            elif model is ApiSnapshot:
                record = ApiSnapshot(
                    source=self.source,
                    source_url=url,
                    request_url=url,
                    request_params={},
                    status_code=response.status_code,
                    response_headers=dict(response.headers),
                    body=compress(response.text),
                )
            else:
                raise ScraperError(f"{self.source}: no snapshot builder for {model}")

            try:
                await self.db.insert(record, model=type(record))
            except DatabaseError as exc:
                raise ScraperError(f"{self.source}: persisting {url} failed") from exc

        return self.get_next_urls(url, response)

    async def run(self) -> None:
        """Lifecycle: BFS over seed_urls(); fetch up to self.concurrency URLs in parallel."""
        frontier: deque[str] = deque(self.seed_urls())
        seen: set[str] = set(frontier)
        dispatched = 0
        max_pages = self.options.max_pages
        in_flight: dict[asyncio.Task[HttpResponse], str] = {}

        # spawn-and-drain: top up to `concurrency` fetches, then drain on FIRST_COMPLETED.
        # max_pages is a hard cap on dispatched fetches — counted at spawn time so no extras slip through.
        try:
            while frontier or in_flight:
                while (
                    frontier
                    and len(in_flight) < self.concurrency
                    and (max_pages == 0 or dispatched < max_pages)
                ):
                    url = frontier.popleft()
                    in_flight[asyncio.create_task(self.fetch(url))] = url
                    dispatched += 1

                if not in_flight:
                    break

                done, _ = await asyncio.wait(in_flight.keys(), return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    url = in_flight.pop(task)
                    response = await task
                    for href in await self.process_response(url, response):
                        if href in seen or not self.should_enqueue(href):
                            continue
                        seen.add(href)
                        frontier.append(href)
                    if self.options.request_delay:
                        await asyncio.sleep(self.options.request_delay)
        finally:
            # drain remaining fetches so a mid-run raise doesn't leak tasks to the loop
            for task in in_flight:
                task.cancel()
            if in_flight:
                await asyncio.gather(*in_flight.keys(), return_exceptions=True)
        return
