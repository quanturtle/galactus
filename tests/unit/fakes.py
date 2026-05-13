"""Shared test doubles for unit tests.

Plain classes (not pytest fixtures) so tests can construct them with custom
canned responses inline. Imported on demand by tests that exercise scraper
hooks without real HTTP or DB.
"""

from collections.abc import Iterable
from typing import Any

from galactus.config import ExtractOptions
from galactus.extract.base_scraper import BaseScraper
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
    """Stand-in for Database — records inserts in memory."""

    def __init__(self) -> None:
        self.inserts: list[tuple[Base, type[Base]]] = []

    async def insert(self, records: Base | Iterable[Base], model: type[Base]) -> None:
        if isinstance(records, Base):
            self.inserts.append((records, model))
        else:
            for r in records:
                self.inserts.append((r, model))
        return


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
