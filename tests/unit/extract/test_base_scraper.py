import asyncio

import pytest

from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.base import Base
from tests.unit.fakes import (
    FakeDatabase,
    FakeHttpClient,
    FakeHttpRequest,
    FakeHttpResponse,
    WiredScraper,
    make_extract_config,
    make_scraper,
)


def test_seed_urls_default_is_base_url_as_request() -> None:
    scraper = make_scraper(BaseScraper)
    seeds = scraper.seed_urls()
    assert len(seeds) == 1
    assert seeds[0].url == "https://example.test"


def test_build_url_default_returns_request_with_config_headers_and_params() -> None:
    scraper = make_scraper(BaseScraper, params={"k": "v"})
    request = scraper.build_url("https://example.test/a?b=2")
    assert isinstance(request, HttpRequest)
    assert request.url == "https://example.test/a?b=2"
    assert request.params == {"k": "v"}
    assert "User-Agent" in request.headers


def test_extract_links_default_returns_a_hrefs_joined_with_response_url() -> None:
    scraper = make_scraper(BaseScraper)
    html = (
        "<html><body>"
        '<a href="/about">about</a>'
        '<a href="https://example.test/contact">contact</a>'
        '<a href="">empty</a>'
        "<p>no link</p>"
        "</body></html>"
    )
    response = FakeHttpResponse(text=html, url="https://example.test/index")
    links = scraper.extract_links(response)  # type: ignore[arg-type]
    assert links == [
        "https://example.test/about",
        "https://example.test/contact",
    ]


def test_should_enqueue_rejects_foreign_hosts_and_ignore_patterns() -> None:
    scraper = make_scraper(BaseScraper, ignore_patterns=[r"/private/"])
    assert scraper.should_enqueue(FakeHttpRequest(url="https://example.test/article/1"))
    assert not scraper.should_enqueue(FakeHttpRequest(url="https://other.test/article/1"))
    assert not scraper.should_enqueue(FakeHttpRequest(url="mailto:a@b.com"))
    assert not scraper.should_enqueue(FakeHttpRequest(url="https://example.test/private/secret"))
    assert not scraper.should_enqueue(FakeHttpRequest(url="https://example.test/file.pdf"))


def test_should_persist_gates_on_patterns() -> None:
    permissive = make_scraper(BaseScraper)
    assert permissive.should_persist(FakeHttpRequest(url="https://example.test/anything"))

    gated = make_scraper(BaseScraper, scrape_patterns=[r"/article/"])
    assert gated.should_persist(FakeHttpRequest(url="https://example.test/article/1"))
    assert not gated.should_persist(FakeHttpRequest(url="https://example.test/about"))


def test_process_response_inserts_html_snapshot_for_html_default() -> None:
    scraper = make_scraper(BaseScraper)
    db: FakeDatabase = scraper.db  # type: ignore[assignment]
    request = FakeHttpRequest(url="https://example.test/x")
    response = FakeHttpResponse(
        text="<html></html>",
        headers={"content-type": "text/html"},
        url="https://example.test/x",
        request=request,
    )
    asyncio.run(scraper.process_response(response))  # type: ignore[arg-type]
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
    request = FakeHttpRequest(url="https://example.test/api", params={"page": "1"})
    response = FakeHttpResponse(
        text='{"ok":true}',
        headers={"content-type": "application/json"},
        url="https://example.test/api",
        request=request,
    )
    asyncio.run(scraper.process_response(response))  # type: ignore[arg-type]
    assert len(db.inserts) == 1
    record, model = db.inserts[0]
    assert model is ApiSnapshot
    assert isinstance(record, ApiSnapshot)
    assert record.source_url == "https://example.test/api"
    assert record.request_params == {"page": "1"}


def test_process_response_raises_scrapererror_for_unknown_snapshot_model() -> None:
    class OtherRecord(Base):
        __abstract__ = True

    class WeirdScraper(BaseScraper):
        snapshot_model = OtherRecord

    scraper = make_scraper(WeirdScraper)
    request = FakeHttpRequest(url="https://example.test/x")
    response = FakeHttpResponse(url="https://example.test/x", request=request)
    with pytest.raises(ScraperError):
        asyncio.run(scraper.process_response(response))  # type: ignore[arg-type]


def test_run_hard_caps_at_max_pages_under_concurrency() -> None:
    # seed fans out to 20 links so the frontier always has more than max_pages to chew on.
    fanout_html = "<html>" + "".join(f'<a href="/p{i}">p{i}</a>' for i in range(20)) + "</html>"
    leaf_html = "<html>leaf</html>"
    responses: dict[str, FakeHttpResponse] = {
        "https://example.test": FakeHttpResponse(text=fanout_html, url="https://example.test"),
    }
    for i in range(20):
        url = f"https://example.test/p{i}"
        responses[url] = FakeHttpResponse(text=leaf_html, url=url)
    http = FakeHttpClient(responses=responses)
    db = FakeDatabase()

    scraper = WiredScraper(make_extract_config(max_pages=10, concurrency=5))
    scraper.wired_http = http
    scraper.wired_db = db
    asyncio.run(scraper.run())

    assert len(http.calls) == 10
    assert len(db.inserts) == 10
    return


def test_run_persists_and_expands_html_bfs() -> None:
    seed_html = '<html><a href="/about">about</a></html>'
    about_html = "<html><body>about page</body></html>"
    http = FakeHttpClient(
        responses={
            "https://example.test": FakeHttpResponse(text=seed_html, url="https://example.test"),
            "https://example.test/about": FakeHttpResponse(
                text=about_html, url="https://example.test/about"
            ),
        }
    )
    db = FakeDatabase()

    scraper = WiredScraper(make_extract_config(concurrency=2))
    scraper.wired_http = http
    scraper.wired_db = db
    asyncio.run(scraper.run())

    fetched = sorted(call.url for call in http.calls)
    assert fetched == ["https://example.test", "https://example.test/about"]
    assert len(db.inserts) == 2
    assert all(model is HtmlSnapshot for _, model in db.inserts)


def test_fetch_sends_request_headers_and_params_through_client() -> None:
    scraper = make_scraper(BaseScraper, params={"static": "yes"})
    http: FakeHttpClient = scraper.http  # type: ignore[assignment]
    request = scraper.build_url("https://example.test/q")
    asyncio.run(scraper.fetch(request))
    assert len(http.calls) == 1
    sent = http.calls[0]
    assert sent.url == "https://example.test/q"
    assert sent.params == {"static": "yes"}
    assert "User-Agent" in sent.headers
