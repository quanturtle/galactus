import asyncio
from collections.abc import Iterable
from typing import Any

import pytest

from galactus.config import ExtractOptions
from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.a_bronze.html_snapshots import HtmlSnapshot
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


def test_default_snapshot_is_html() -> None:
    scraper = make_scraper(BaseScraper)
    resp = FakeResponse(text="<html></html>", headers={"content-type": "text/html"})
    record = scraper.build_snapshot("https://example.test/x", resp)  # type: ignore[arg-type]
    assert isinstance(record, HtmlSnapshot)
    assert record.source_url == "https://example.test/x"


def test_api_snapshot_routing() -> None:
    class ApiScraper(BaseScraper):
        snapshot_model = ApiSnapshot

    scraper = make_scraper(ApiScraper)
    resp = FakeResponse(text='{"ok":true}', headers={"content-type": "application/json"})
    record = scraper.build_snapshot("https://example.test/api", resp)  # type: ignore[arg-type]
    assert isinstance(record, ApiSnapshot)
    assert record.source_url == "https://example.test/api"


def test_unknown_snapshot_model_raises_scrapererror() -> None:
    class OtherRecord(Base):
        __abstract__ = True

    class WeirdScraper(BaseScraper):
        snapshot_model = OtherRecord

    scraper = make_scraper(WeirdScraper)
    resp = FakeResponse()
    with pytest.raises(ScraperError):
        scraper.build_snapshot("https://example.test/x", resp)  # type: ignore[arg-type]


def test_seeds_default_is_base_url() -> None:
    scraper = make_scraper(BaseScraper)
    assert scraper.seeds() == ["https://example.test"]


def test_build_url_default_is_identity() -> None:
    scraper = make_scraper(BaseScraper)
    assert scraper.build_url("https://example.test/a?b=2") == "https://example.test/a?b=2"


def test_should_persist_gates_on_patterns() -> None:
    permissive = make_scraper(BaseScraper)
    assert permissive.should_persist("https://example.test/anything")

    gated = make_scraper(BaseScraper, scrape_url_patterns=[r"/article/"])
    assert gated.should_persist("https://example.test/article/1")
    assert not gated.should_persist("https://example.test/about")


def test_should_enqueue_rejects_skip_prefixes_and_other_schemes() -> None:
    scraper = make_scraper(BaseScraper)
    assert not scraper._should_enqueue("mailto:a@b.com")
    assert not scraper._should_enqueue("javascript:void(0)")
    assert not scraper._should_enqueue("ftp://example.test/x")
    assert scraper._should_enqueue("https://example.test/x")


def test_run_persists_and_expands_html_bfs() -> None:
    class Scraper(BaseScraper):
        pass

    options = ExtractOptions(base_url="https://example.test")
    seed_html = '<html><a href="/about">about</a></html>'
    about_html = "<html><body>about page</body></html>"
    http = FakeHttpClient(
        responses={
            "https://example.test": FakeResponse(text=seed_html),
            "https://example.test/about": FakeResponse(text=about_html),
        }
    )
    db = FakeDatabase()
    scraper = Scraper(
        source="testsrc",
        http=http,  # type: ignore[arg-type]
        db=db,  # type: ignore[arg-type]
        options=options,
        concurrency=2,
    )
    asyncio.run(scraper.run())

    fetched = sorted(http.calls)
    assert fetched == ["https://example.test", "https://example.test/about"]
    assert len(db.inserts) == 2
    assert all(model is HtmlSnapshot for _, model in db.inserts)
