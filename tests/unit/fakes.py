"""Shared test doubles for unit tests.

Plain classes (not pytest fixtures) so tests can construct them with custom
canned responses inline. Imported on demand by tests that exercise scraper
hooks without real HTTP or DB.
"""

from collections.abc import Iterable, Mapping
from typing import Any

from galactus.config import ExtractConfig, TransformConfig
from galactus.extract.base_scraper import BaseScraper
from galactus.transform.base_parser import BaseParser
from sql.base import Base


class FakeResponse:
    """Stand-in for HttpResponse — same duck-typed surface scrapers read."""

    def __init__(
        self,
        text: str = "",
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        json_body: Any = None,
    ) -> None:
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}
        self._json = json_body

    def json(self) -> Any:
        return self._json


class FakeHttpClient:
    """Stand-in for HttpClient — returns a canned response per URL."""

    def __init__(self, responses: dict[str, FakeResponse] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[str] = []

    async def get(
        self,
        url: str,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> FakeResponse:
        self.calls.append(url)
        return self.responses.get(url, FakeResponse(text=""))

    async def __aenter__(self) -> "FakeHttpClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return


class FakeDatabase:
    """Stand-in for Database — records inserts and load_unparsed calls in memory."""

    def __init__(
        self,
        load_unparsed_results: list[Base] | None = None,
        load_raises: Exception | None = None,
        insert_raises: Exception | None = None,
    ) -> None:
        self.inserts: list[tuple[Base, type[Base]]] = []
        self.load_calls: list[tuple[type[Base], type[Base], str]] = []
        self._load_unparsed_results = load_unparsed_results or []
        self._load_raises = load_raises
        self._insert_raises = insert_raises

    async def insert(self, records: Base | Iterable[Base], model: type[Base]) -> None:
        if self._insert_raises is not None:
            raise self._insert_raises
        if isinstance(records, Base):
            self.inserts.append((records, model))
        else:
            for r in records:
                self.inserts.append((r, model))
        return

    async def load_unparsed(
        self,
        bronze_model: type[Base],
        silver_model: type[Base],
        source: str,
    ) -> list[Base]:
        self.load_calls.append((bronze_model, silver_model, source))
        if self._load_raises is not None:
            raise self._load_raises
        return list(self._load_unparsed_results)

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
