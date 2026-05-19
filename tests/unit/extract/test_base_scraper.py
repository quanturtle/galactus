import asyncio

from bs4 import BeautifulSoup

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.a_bronze.html_snapshots import HtmlSnapshot
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
    scraper = make_scraper(BaseScraper)
    request = scraper.build_url("https://example.test/a?b=2", params={"k": "v"})
    assert isinstance(request, HttpRequest)
    assert request.url == "https://example.test/a?b=2"
    assert request.params == {"k": "v"}
    assert "User-Agent" in request.headers


def test_build_url_strips_tracking_params_and_lowercases_host() -> None:
    scraper = make_scraper(BaseScraper)

    # utm_*, fbclid, gclid drop out; non-tracking params remain in original order
    tracked = scraper.build_url(
        "https://Example.Test/A?id=1&utm_source=foo&fbclid=zzz&utm_medium=email"
    )
    assert tracked.url == "https://example.test/A?id=1"

    # url with only tracking params collapses to a bare path (no trailing "?")
    only_tracking = scraper.build_url("https://example.test/p?gclid=abc&utm_campaign=spring")
    assert only_tracking.url == "https://example.test/p"

    # fragments are dropped — they don't travel over HTTP and would split the BFS seen set
    with_fragment = scraper.build_url("https://example.test/p?utm_source=x#section")
    assert with_fragment.url == "https://example.test/p"


def test_build_url_deduplicates_links_that_differ_only_in_tracking_params() -> None:
    # canonicalization runs inside build_url, so HttpRequest equality collapses
    # tracked and untracked variants of the same URL into one frontier entry
    scraper = make_scraper(BaseScraper)
    a = scraper.build_url("https://example.test/p?id=1&utm_source=newsletter")
    b = scraper.build_url("https://example.test/p?id=1")
    assert a.url == b.url


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
    soup = scraper.html_parser.parse(html)
    links = scraper.extract_links(response, soup)  # type: ignore[arg-type]
    assert links == [
        "https://example.test/about",
        "https://example.test/contact",
    ]


def test_extract_links_bare_relative_resolves_against_base_url_not_response() -> None:
    # bare-relative hrefs (no leading slash, no scheme) must resolve against base_url so
    # `catalogo/foo` on `/catalogo/x` doesn't recurse into `/catalogo/catalogo/foo`.
    scraper = make_scraper(BaseScraper)
    html = '<a href="catalogo/foo-p1">foo</a>'
    response = FakeHttpResponse(text=html, url="https://example.test/catalogo/x-p9")
    soup = scraper.html_parser.parse(html)
    links = scraper.extract_links(response, soup)  # type: ignore[arg-type]
    assert links == ["https://example.test/catalogo/foo-p1"]


def test_should_enqueue_rejects_foreign_hosts_and_ignore_patterns() -> None:
    scraper = make_scraper(BaseScraper, ignore_patterns=[r"/private/"])
    assert scraper.should_enqueue(FakeHttpRequest(url="https://example.test/article/1"))
    assert not scraper.should_enqueue(FakeHttpRequest(url="https://other.test/article/1"))
    assert not scraper.should_enqueue(FakeHttpRequest(url="mailto:a@b.com"))
    assert not scraper.should_enqueue(FakeHttpRequest(url="https://example.test/private/secret"))
    assert not scraper.should_enqueue(FakeHttpRequest(url="https://example.test/file.pdf"))


def test_should_enqueue_rejects_paths_with_repeated_segments() -> None:
    scraper = make_scraper(BaseScraper)
    assert not scraper.should_enqueue(
        FakeHttpRequest(url="https://example.test/catalogo/catalogo/foo-p1")
    )
    assert not scraper.should_enqueue(
        FakeHttpRequest(url="https://example.test/promociones/catalogo/promociones/foo-p1")
    )
    # unrelated repeated tokens between different segments are fine
    assert scraper.should_enqueue(FakeHttpRequest(url="https://example.test/a/b/c"))


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
    assert record.request_url == "https://example.test/x"
    assert record.request_headers == {}
    assert record.request_params == {}


def test_process_response_inserts_api_snapshot_when_bronze_model_is_api() -> None:
    class ApiScraper(BaseScraper):
        bronze_model = ApiSnapshot

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
    assert record.request_url == "https://example.test/api"
    assert record.request_params == {"page": "1"}


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


def test_run_skips_links_already_visited_today() -> None:
    # seed lists two links — one already in today's bronze, one fresh.
    seed_html = '<html><a href="/old">old</a><a href="/new">new</a></html>'
    leaf_html = "<html>leaf</html>"
    http = FakeHttpClient(
        responses={
            "https://example.test": FakeHttpResponse(text=seed_html, url="https://example.test"),
            "https://example.test/old": FakeHttpResponse(
                text=leaf_html, url="https://example.test/old"
            ),
            "https://example.test/new": FakeHttpResponse(
                text=leaf_html, url="https://example.test/new"
            ),
        }
    )
    db = FakeDatabase(load_visited_results=[("https://example.test/old", {})])

    scraper = WiredScraper(make_extract_config(concurrency=2))
    scraper.wired_http = http
    scraper.wired_db = db
    asyncio.run(scraper.run())

    fetched = sorted(call.url for call in http.calls)
    assert fetched == ["https://example.test", "https://example.test/new"]
    assert db.visited_calls == [(HtmlSnapshot, "testsrc")]


def test_run_skips_seed_when_already_in_todays_bronze() -> None:
    # seed itself appears in today's visited set — skip it so a same-day rerun
    # doesn't write duplicate bronze rows for the section entrypoint.
    seed_html = '<html><a href="/new">new</a></html>'
    leaf_html = "<html>leaf</html>"
    http = FakeHttpClient(
        responses={
            "https://example.test": FakeHttpResponse(text=seed_html, url="https://example.test"),
            "https://example.test/new": FakeHttpResponse(
                text=leaf_html, url="https://example.test/new"
            ),
        }
    )
    db = FakeDatabase(load_visited_results=[("https://example.test", {})])

    scraper = WiredScraper(make_extract_config(concurrency=2))
    scraper.wired_http = http
    scraper.wired_db = db
    asyncio.run(scraper.run())

    # seed skipped → no fetch and so no link expansion either
    assert http.calls == []


def test_run_skips_api_page_already_visited_today() -> None:
    # paginating API scraper: same base URL, page differentiator lives in params.
    # bronze row for offset=0 must dedupe against the seed request for offset=0
    # while offset=100 remains fresh.
    base_url = "https://api.example.test/feed"

    class PagingApiScraper(BaseScraper):
        bronze_model = ApiSnapshot

        def build_url(  # type: ignore[override]
            self,
            offset: int | None = None,
            url: str | None = None,
            params: dict[str, object] | None = None,
        ) -> HttpRequest:
            return HttpRequest(
                url=url if url is not None else base_url,
                headers=dict(self.config.headers),
                params=params if params is not None else {"offset": str(offset)},
            )

        def seed_urls(self) -> list[HttpRequest]:
            return [self.build_url(0), self.build_url(100)]

        def get_next_urls(
            self, response: HttpResponse, soup: BeautifulSoup | None = None
        ) -> list[HttpRequest]:
            return []

    http = FakeHttpClient(
        responses={
            base_url: FakeHttpResponse(text='{"ok":true}', url=base_url),
        }
    )
    # already visited: offset=0
    db = FakeDatabase(load_visited_results=[(base_url, {"offset": "0"})])

    class PagingApiWired(PagingApiScraper):
        wired_http: FakeHttpClient
        wired_db: FakeDatabase

        def make_http_client(self) -> FakeHttpClient:  # type: ignore[override]
            return self.wired_http

        def make_database(self) -> FakeDatabase:  # type: ignore[override]
            return self.wired_db

    scraper = PagingApiWired(
        make_extract_config(base_url=base_url, allowed_domains=frozenset({"api.example.test"}))
    )
    scraper.wired_http = http
    scraper.wired_db = db
    asyncio.run(scraper.run())

    # offset=0 seed is skipped (in today's bronze); offset=100 stays fresh.
    # The assertion also checks that seen_today builds matching hashes via
    # build_url(url=, params=) so the paginated request doesn't crash on a
    # positional URL string.
    assert len(http.calls) == 1
    assert http.calls[0].params["offset"] == "100"


def test_fetch_sends_request_headers_and_params_through_client() -> None:
    scraper = make_scraper(BaseScraper)
    http: FakeHttpClient = scraper.http  # type: ignore[assignment]
    request = scraper.build_url("https://example.test/q", params={"static": "yes"})
    asyncio.run(scraper.fetch(request))
    assert len(http.calls) == 1
    sent = http.calls[0]
    assert sent.url == "https://example.test/q"
    assert sent.params == {"static": "yes"}
    assert "User-Agent" in sent.headers
