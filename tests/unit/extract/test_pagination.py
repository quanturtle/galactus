"""Per-scraper pagination strategies.

Each section covers one pagination shape exercised through BaseScraper.get_next_urls.
Strategies shared by multiple scrapers are parametrized across the classes;
strategies unique to one scraper get direct tests.
"""

import json

import pytest

from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from galactus.extract.scrapers.noticias.hoy import Scraper as HoyScraper
from galactus.extract.scrapers.noticias.lanacion import Scraper as LaNacionScraper
from galactus.extract.scrapers.noticias.megacadena import Scraper as MegacadenaScraper
from galactus.extract.scrapers.supermercados.biggie import Scraper as BiggieScraper
from galactus.extract.scrapers.supermercados.grutter import Scraper as GrutterScraper
from galactus.infra.http import HttpRequest
from tests.unit.fakes import FakeHttpRequest, FakeHttpResponse, make_scraper


def _scraper(
    cls: type[BaseScraper],
    base_url: str,
    *,
    concurrency: int = 1,
    params: dict[str, str] | None = None,
) -> BaseScraper:
    return make_scraper(
        cls,
        base_url=base_url,
        params=params or {},
        concurrency=concurrency,
    )


def _response_for(seed: HttpRequest, **kwargs) -> FakeHttpResponse:
    # stamp the originating request onto the response so get_next_urls can read it back
    return FakeHttpResponse(url=seed.url, request=seed, **kwargs)


# WP totalpages paging — header-driven, shared across three scrapers.

WP_BASE_URL = "https://example.test/wp-json/wp/v2/posts"
WP_SCRAPERS = [HoyScraper, MegacadenaScraper, GrutterScraper]


@pytest.mark.parametrize("cls", WP_SCRAPERS)
def test_wp_totalpages_emits_pages_when_header_present(cls: type[BaseScraper]) -> None:
    scraper = _scraper(cls, WP_BASE_URL)
    seed = scraper.seed_urls()[0]
    response = _response_for(seed, headers={"x-wp-totalpages": "5"})

    requests = scraper.get_next_urls(response)  # type: ignore[arg-type]

    assert requests == [scraper.build_url(p) for p in range(2, 6)]
    return


@pytest.mark.parametrize("cls", WP_SCRAPERS)
def test_wp_totalpages_raises_when_header_missing(cls: type[BaseScraper]) -> None:
    scraper = _scraper(cls, WP_BASE_URL)
    seed = scraper.seed_urls()[0]
    response = _response_for(seed, headers={})

    with pytest.raises(ScraperError):
        scraper.get_next_urls(response)  # type: ignore[arg-type]
    return


# Biggie skip/count paging — JSON body's `count` drives skip offsets.

BIGGIE_BASE_URL = "https://api.app.biggie.com.py/api/articles"


def test_biggie_skip_emits_offsets_when_count_present() -> None:
    scraper = _scraper(BiggieScraper, BIGGIE_BASE_URL, params={"take": "100"})
    seed = scraper.seed_urls()[0]
    response = _response_for(seed, json_body={"count": 350})

    requests = scraper.get_next_urls(response)  # type: ignore[arg-type]

    assert requests == [scraper.build_url(skip) for skip in (100, 200, 300)]
    return


def test_biggie_skip_raises_when_count_missing() -> None:
    scraper = _scraper(BiggieScraper, BIGGIE_BASE_URL, params={"take": "100"})
    seed = scraper.seed_urls()[0]
    response = _response_for(seed, json_body={})

    with pytest.raises(ScraperError):
        scraper.get_next_urls(response)  # type: ignore[arg-type]
    return


def test_biggie_seed_request_carries_skip_param_not_in_url() -> None:
    scraper = _scraper(BiggieScraper, BIGGIE_BASE_URL, params={"take": "100"})
    seed = scraper.seed_urls()[0]
    assert seed.url == BIGGIE_BASE_URL
    assert seed.params["skip"] == "0"
    return


# LaNacion concurrency-offset paging — each full page emits `concurrency` more offsets.

LANACION_BASE_URL = "https://www.lanacion.com.py/pf/api/v3/content/fetch/content-search-feed-full"


def test_lanacion_emits_concurrency_offsets_on_full_page() -> None:
    scraper = _scraper(
        LaNacionScraper, LANACION_BASE_URL, concurrency=5, params={"feedSize": "100"}
    )
    seed = scraper.seed_urls()[0]
    response = _response_for(seed, json_body={"content_elements": [{} for _ in range(100)]})

    requests = scraper.get_next_urls(response)  # type: ignore[arg-type]

    assert len(requests) == 5
    offsets = []
    for req in requests:
        blob = json.loads(req.params["query"])
        offsets.append(int(blob["feedFrom"]))
    assert offsets == [100, 200, 300, 400, 500]
    return


def test_lanacion_empty_on_short_page() -> None:
    scraper = _scraper(
        LaNacionScraper, LANACION_BASE_URL, concurrency=5, params={"feedSize": "100"}
    )
    seed = scraper.seed_urls()[0]
    response = _response_for(seed, json_body={"content_elements": [{} for _ in range(50)]})

    assert scraper.get_next_urls(response) == []  # type: ignore[arg-type]
    return


def test_lanacion_raises_on_malformed_seed() -> None:
    scraper = _scraper(LaNacionScraper, LANACION_BASE_URL, params={"feedSize": "100"})
    # response.request has no `query` param — get_next_urls should fail loudly
    bare_request = FakeHttpRequest(url=LANACION_BASE_URL)
    response = FakeHttpResponse(
        url=LANACION_BASE_URL,
        request=bare_request,
        json_body={"content_elements": [{} for _ in range(100)]},
    )

    with pytest.raises(KeyError):
        scraper.get_next_urls(response)  # type: ignore[arg-type]
    return
