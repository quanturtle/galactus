"""Shared test doubles for unit tests.

Plain classes (not pytest fixtures) so tests can construct them with custom
canned responses inline. Imported on demand by tests that exercise scraper
hooks without real HTTP or DB.
"""

from collections.abc import AsyncIterator, Iterable
from typing import Any

from zstandard import ZstdCompressor, ZstdDecompressor

from galactus.config import ExtractConfig, TransformConfig
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest
from galactus.transform.base_parser import BaseParser
from sql.base import Base


class FakeHttpRequest(HttpRequest):
    """Test stand-in for HttpRequest — same surface, kept here for naming symmetry
    with FakeHttpResponse. Inherits HttpRequest's frozen-ish storage and hashing
    so it interchanges freely with production HttpRequest instances in BFS sets.
    """

    pass


class FakeHttpResponse:
    """Stand-in for HttpResponse — same duck-typed surface scrapers read.

    Carries a `request` attribute mirroring HttpResponse.request so hooks that
    read `response.request.params` (Arc Publishing paginators) work unchanged.
    """

    def __init__(
        self,
        text: str = "",
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        json_body: Any = None,
        url: str = "",
        request: HttpRequest | None = None,
    ) -> None:
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}
        self._json = json_body
        self.url = url
        self.request = request if request is not None else FakeHttpRequest(url=url)

    def json(self) -> Any:
        return self._json


class FakeHttpClient:
    """Stand-in for HttpClient — returns a canned response per URL.

    Responses are keyed by request URL. The originating request is stamped onto
    the returned FakeHttpResponse so process_response and get_next_urls can
    read it back.
    """

    def __init__(self, responses: dict[str, FakeHttpResponse] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[HttpRequest] = []

    async def get(self, request: HttpRequest) -> FakeHttpResponse:
        self.calls.append(request)
        response = self.responses.get(request.url, FakeHttpResponse(text=""))
        response.request = request
        if not response.url:
            response.url = request.url
        return response

    async def __aenter__(self) -> "FakeHttpClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return


class FakeDatabase:
    """Stand-in for Database — records inserts and load_unparsed calls in memory."""

    def __init__(
        self,
        load_unparsed_results: list[Base] | None = None,
        load_visited_results: list[tuple[str, dict[str, Any]]] | None = None,
        load_raises: Exception | None = None,
        insert_raises: Exception | None = None,
    ) -> None:
        self.inserts: list[tuple[Base, type[Base]]] = []
        self.insert_call_count = 0
        self.load_calls: list[tuple[type[Base], type[Base], str]] = []
        self.visited_calls: list[tuple[type[Base], str]] = []
        self._load_unparsed_results = load_unparsed_results or []
        self._load_visited_results = load_visited_results or []
        self._load_raises = load_raises
        self._insert_raises = insert_raises
        self.compressor = ZstdCompressor(level=6)
        self.decompressor = ZstdDecompressor()

    def compress(self, text: str) -> bytes:
        return self.compressor.compress(text.encode("utf-8"))

    def decompress(self, blob: bytes) -> str:
        return self.decompressor.decompress(blob).decode("utf-8")

    async def insert(self, records: Base | Iterable[Base], model: type[Base]) -> None:
        self.insert_call_count += 1
        if self._insert_raises is not None:
            raise self._insert_raises
        if isinstance(records, Base):
            self.inserts.append((records, model))
        else:
            for r in records:
                self.inserts.append((r, model))
        return

    async def stream_unparsed(
        self,
        bronze_model: type[Base],
        silver_model: type[Base],
        source: str,
        chunk_size: int = 100,
    ) -> AsyncIterator[Base]:
        self.load_calls.append((bronze_model, silver_model, source))
        if self._load_raises is not None:
            raise self._load_raises
        for row in list(self._load_unparsed_results):
            yield row

    async def load_visited_requests(
        self,
        model: type[Base],
        source: str,
    ) -> list[tuple[str, dict[str, Any]]]:
        self.visited_calls.append((model, source))
        if self._load_raises is not None:
            raise self._load_raises
        return list(self._load_visited_results)

    async def __aenter__(self) -> "FakeDatabase":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return


def make_extract_config(**overrides: Any) -> ExtractConfig:
    """Construct a minimal ExtractConfig for tests."""
    base: dict[str, Any] = {
        "source": "testsrc",
        "database_url": "placeholder://test",
        "scraper": "unused",
        "concurrency": 1,
        "base_url": "https://example.test",
        "allowed_domains": frozenset({"example.test"}),
    }
    base.update(overrides)
    return ExtractConfig.model_validate(base)


def make_transform_config(**overrides: Any) -> TransformConfig:
    """Construct a minimal TransformConfig for tests."""
    base: dict[str, Any] = {
        "source": "testsrc",
        "database_url": "placeholder://test",
        "parser": "unused",
    }
    base.update(overrides)
    return TransformConfig.model_validate(base)


def make_scraper(scraper_cls: type[BaseScraper], **option_overrides: Any) -> BaseScraper:
    """Construct a scraper and attach fake http+db as instance attributes.

    Suitable for tests that exercise hooks directly (extract_links,
    should_enqueue, get_next_urls, process_response). Tests that call
    scraper.run() should instead use WiredScraper, which routes the fakes
    through the make_http_client/make_database hooks that run() opens.
    """
    config = make_extract_config(**option_overrides)
    scraper = scraper_cls(config)
    scraper.http = FakeHttpClient()  # type: ignore[assignment]
    scraper.db = FakeDatabase()  # type: ignore[assignment]
    return scraper


def make_parser(
    parser_cls: type[BaseParser],
    *,
    source: str = "testsrc",
    db: FakeDatabase | None = None,
    **option_overrides: Any,
) -> BaseParser:
    """Construct a parser and attach a fake db as an instance attribute.

    Suitable for tests that exercise hooks directly (decode, build_entities,
    parse_records). Tests that call parser.run() should instead define a
    parser subclass that overrides make_database, see WiredScraper for the
    extract counterpart pattern.
    """
    config = make_transform_config(source=source, **option_overrides)
    parser = parser_cls(config)
    parser.db = db if db is not None else FakeDatabase()  # type: ignore[assignment]
    return parser


class WiredScraper(BaseScraper):
    """Test scraper that routes the fake http+db through the make_* hooks.

    Use for tests that exercise BaseScraper.run() end-to-end. Caller sets
    wired_http and wired_db before constructing, then run() opens them
    via make_http_client / make_database.
    """

    wired_http: FakeHttpClient
    wired_db: FakeDatabase

    def make_http_client(self) -> FakeHttpClient:  # type: ignore[override]
        return self.wired_http

    def make_database(self) -> FakeDatabase:  # type: ignore[override]
        return self.wired_db
