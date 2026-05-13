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

# persist full HTML bodies in bronze.html_snapshots (the parsers need them).
STORE_HTML_BODY = True


class BaseScraper:
    """Template Method base for all scrapers.

    run() owns the BFS over seed_urls(): fetch a URL, persist it if eligible,
    then expand the frontier with the page's next URLs.

    Public hook surface — override in subclasses, every one ships a default:
        snapshot_model      class var; default HtmlSnapshot. API scrapers set
                            to ApiSnapshot. The discriminator process_response
                            routes on when building the bronze record.
        seed_urls()         initial frontier; default [options.base_url].
        fetch(url)          one HTTP GET; default delegates to self.http.get
                            and wraps HttpError as ScraperError.
        extract_links(url, response) raw candidate links from a response;
                            default scrapes every <a href>, joined with url.
        build_url(*args, **kwargs) raw link -> request URL. Default identity
                            (returns args[0]). Paginated-API subclasses
                            re-shape the signature (e.g. build_url(page),
                            build_url(section, offset)) and call their own
                            build_url from seed_urls() / get_next_urls().
        get_next_urls(url, response) URLs to add to the frontier after
                            visiting one; default = [build_url(l) for l in
                            extract_links(url, response)].
        should_enqueue(url) gate before adding a URL to the frontier;
                            default = same host, skip media extensions,
                            and no ignore_url_patterns match.
        should_persist(url) gate before persisting a fetched URL;
                            default = scrape-pattern check (allow-all when
                            no patterns).
        process_response(url, response) per-response orchestration; default
                            persists the snapshot if should_persist(url) and
                            returns get_next_urls(url, response).

    Private internals — not extension points: run.
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
    def seed_urls(self) -> list[str]:
        return [self.options.base_url]

    # hook: one HTTP GET. Default delegates to the injected HttpClient and
    # converts low-level HttpError into a domain ScraperError.
    async def fetch(self, url: str) -> HttpResponse:
        try:
            return await self.http.get(url)
        except HttpError as exc:
            raise ScraperError(f"{self.source}: GET {url} failed") from exc

    # hook: a fetched page -> raw candidate links (absolute or relative).
    # No filtering here — get_next_urls canonicalizes via build_url and
    # should_enqueue gates before enqueueing. Default scrapes every
    # <a href>; a JSON body yields none.
    def extract_links(self, url: str, response: HttpResponse) -> list[str]:
        soup = BeautifulSoup(response.text, "html.parser")
        out: list[str] = []
        for a in soup.find_all("a", href=True):
            href = str(a["href"]).strip()
            if not href:
                continue
            out.append(urljoin(url, href))
        return out

    # hook: raw link -> request URL. Default returns the first positional
    # arg as-is. Paginating subclasses override with a different signature
    # (e.g. build_url(self, page: int)) and call their own build_url from
    # inside seed_urls() / get_next_urls().
    def build_url(self, *args, **kwargs) -> str:
        return args[0]

    # hook: URLs to add to the frontier after visiting one. Default composes
    # extract_links + build_url; override to plug in pagination state.
    def get_next_urls(self, url: str, response: HttpResponse) -> list[str]:
        return [self.build_url(link) for link in self.extract_links(url, response)]

    # hook: gate before adding a URL to the frontier. Default = same host,
    # skip media extensions, no ignore_url_patterns match. Naturally rejects
    # mailto:/tel:/javascript: (their urlparse netloc is empty).
    def should_enqueue(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc not in self._allowed_hosts:
            return False
        if Path(parsed.path).suffix.lower() in SKIP_EXTENSIONS:
            return False
        return not any(p.search(url) for p in self._ignore_patterns)

    # hook: gate persistence — return True to snapshot this url, False to skip
    def should_persist(self, url: str) -> bool:
        if not self._scrape_patterns:
            return True
        return any(p.search(url) for p in self._scrape_patterns)

    # hook: per-response orchestration. Persist the snapshot if eligible,
    # then return the next URLs to enqueue. Override entirely for a fully
    # custom record shape or to gate persistence on response content.
    async def process_response(self, url: str, response: HttpResponse) -> list[str]:
        # persist if eligible — route snapshot construction on snapshot_model
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
        # init frontier
        frontier: deque[str] = deque(self.seed_urls())
        seen: set[str] = set(frontier)
        fetched = 0
        max_pages = self.options.max_pages
        in_flight: dict[asyncio.Task[HttpResponse], str] = {}

        # spawn-and-drain loop: top up to `concurrency` fetch tasks; drain as
        # they finish — for each completed fetch, run process_response, fold
        # its next URLs into the frontier, then self-throttle. max_pages is a
        # soft cap on dispatched tasks: once spawning stops, up to
        # concurrency-1 in-flight tasks may still complete after the cap.
        while frontier or in_flight:
            while (
                frontier
                and len(in_flight) < self.concurrency
                and (max_pages == 0 or fetched < max_pages)
            ):
                url = frontier.popleft()
                in_flight[asyncio.create_task(self.fetch(url))] = url

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
                fetched += 1
        return
