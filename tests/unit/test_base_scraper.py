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


def test_seed_urls_default_is_base_url() -> None:
    scraper = make_scraper(BaseScraper)
    assert scraper.seed_urls() == ["https://example.test"]


def test_build_url_default_is_identity() -> None:
    scraper = make_scraper(BaseScraper)
    assert scraper.build_url("https://example.test/a?b=2") == "https://example.test/a?b=2"


def test_extract_links_default_returns_a_hrefs_joined_with_url() -> None:
    scraper = make_scraper(BaseScraper)
    html = (
        "<html><body>"
        '<a href="/about">about</a>'
        '<a href="https://example.test/contact">contact</a>'
        '<a href="">empty</a>'
        "<p>no link</p>"
        "</body></html>"
    )
    response = FakeResponse(text=html)
    links = scraper.extract_links("https://example.test/index", response)  # type: ignore[arg-type]
    assert links == [
        "https://example.test/about",
        "https://example.test/contact",
    ]


def test_should_enqueue_rejects_foreign_hosts_and_ignore_patterns() -> None:
    scraper = make_scraper(BaseScraper, ignore_url_patterns=[r"/private/"])
    assert scraper.should_enqueue("https://example.test/article/1")
    assert not scraper.should_enqueue("https://other.test/article/1")
    assert not scraper.should_enqueue("mailto:a@b.com")
    assert not scraper.should_enqueue("https://example.test/private/secret")
    assert not scraper.should_enqueue("https://example.test/file.pdf")


def test_should_persist_gates_on_patterns() -> None:
    permissive = make_scraper(BaseScraper)
    assert permissive.should_persist("https://example.test/anything")

    gated = make_scraper(BaseScraper, scrape_url_patterns=[r"/article/"])
    assert gated.should_persist("https://example.test/article/1")
    assert not gated.should_persist("https://example.test/about")


def test_process_response_inserts_html_snapshot_for_html_default() -> None:
    scraper = make_scraper(BaseScraper)
    db: FakeDatabase = scraper.db  # type: ignore[assignment]
    response = FakeResponse(text="<html></html>", headers={"content-type": "text/html"})
    asyncio.run(scraper.process_response("https://example.test/x", response))  # type: ignore[arg-type]
    assert len(db.inserts) == 1
    record, model = db.inserts[0]
    assert model is HtmlSnapshot
    assert isinstance(record, HtmlSnapshot)
    assert record.source_url == "https://example.test/x"


def test_process_response_inserts_api_snapshot_when_snapshot_model_is_api() -> None:
    class ApiScraper(BaseScraper):
        snapshot_model = ApiSnapshot

    scraper = make_scraper(ApiScraper)
    db: FakeDatabase = scraper.db  # type: ignore[assignment]
    response = FakeResponse(text='{"ok":true}', headers={"content-type": "application/json"})
    asyncio.run(scraper.process_response("https://example.test/api", response))  # type: ignore[arg-type]
    assert len(db.inserts) == 1
    record, model = db.inserts[0]
    assert model is ApiSnapshot
    assert isinstance(record, ApiSnapshot)
    assert record.source_url == "https://example.test/api"


def test_process_response_raises_scrapererror_for_unknown_snapshot_model() -> None:
    class OtherRecord(Base):
        __abstract__ = True

    class WeirdScraper(BaseScraper):
        snapshot_model = OtherRecord

    scraper = make_scraper(WeirdScraper)
    response = FakeResponse()
    with pytest.raises(ScraperError):
        asyncio.run(scraper.process_response("https://example.test/x", response))  # type: ignore[arg-type]


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
