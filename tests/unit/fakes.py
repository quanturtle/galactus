"""Shared test doubles for unit tests.

Plain classes (not pytest fixtures) so tests can construct them with custom
canned responses inline. Imported on demand by tests that exercise scraper
hooks without real HTTP or DB.
"""

from collections.abc import Iterable
from typing import Any

from galactus.config import ExtractOptions, TransformOptions
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

    async def get(self, url: str) -> FakeResponse:
        self.calls.append(url)
        return self.responses.get(url, FakeResponse(text=""))


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


def make_scraper(scraper_cls: type[BaseScraper], **option_overrides: Any) -> BaseScraper:
    """Construct a scraper of the given class with fake http+db and minimal options."""
    options = ExtractOptions(base_url="https://example.test", **option_overrides)
    return scraper_cls(
        source="testsrc",
        http=FakeHttpClient(),  # type: ignore[arg-type]
        db=FakeDatabase(),  # type: ignore[arg-type]
        options=options,
        concurrency=1,
    )


def make_parser(
    parser_cls: type[BaseParser],
    *,
    source: str = "testsrc",
    db: FakeDatabase | None = None,
    **option_overrides: Any,
) -> BaseParser:
    """Construct a parser of the given class with a fake db and minimal options."""
    options = TransformOptions(**option_overrides)
    return parser_cls(
        source=source,
        db=db if db is not None else FakeDatabase(),  # type: ignore[arg-type]
        options=options,
    )
