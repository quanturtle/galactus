"""Per-scraper pagination strategies.

Each section covers one pagination shape exercised through BaseScraper.get_next_urls.
Strategies shared by multiple scrapers are parametrized across the classes;
strategies unique to one scraper get direct tests.
"""

import json
from urllib.parse import parse_qs, urlparse

import pytest

from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from galactus.extract.scrapers.noticias.hoy import Scraper as HoyScraper
from galactus.extract.scrapers.noticias.lanacion import Scraper as LaNacionScraper
from galactus.extract.scrapers.noticias.megacadena import Scraper as MegacadenaScraper
from galactus.extract.scrapers.supermercados.biggie import Scraper as BiggieScraper
from galactus.extract.scrapers.supermercados.grutter import Scraper as GrutterScraper
from tests.unit.fakes import FakeResponse, make_scraper


def _scraper(
    cls: type[BaseScraper],
    base_url: str,
    *,
    concurrency: int = 1,
    page_size: int = 100,
) -> BaseScraper:
    return make_scraper(
        cls,
        base_url=base_url,
        page_size=page_size,
        concurrency=concurrency,
    )


# WP totalpages paging — header-driven, shared across three scrapers.

WP_BASE_URL = "https://example.test/wp-json/wp/v2/posts"
WP_SCRAPERS = [HoyScraper, MegacadenaScraper, GrutterScraper]


@pytest.mark.parametrize("cls", WP_SCRAPERS)
def test_wp_totalpages_emits_pages_when_header_present(cls: type[BaseScraper]) -> None:
    scraper = _scraper(cls, WP_BASE_URL)
    seed = scraper.seed_urls()[0]
    response = FakeResponse(headers={"x-wp-totalpages": "5"})

    urls = scraper.get_next_urls(seed, response)  # type: ignore[arg-type]

    assert urls == [scraper.build_url(p) for p in range(2, 6)]
    return


@pytest.mark.parametrize("cls", WP_SCRAPERS)
def test_wp_totalpages_raises_when_header_missing(cls: type[BaseScraper]) -> None:
    scraper = _scraper(cls, WP_BASE_URL)
    seed = scraper.seed_urls()[0]
    response = FakeResponse(headers={})

    with pytest.raises(ScraperError):
        scraper.get_next_urls(seed, response)  # type: ignore[arg-type]
    return


# Biggie skip/count paging — JSON body's `count` drives skip offsets.

BIGGIE_BASE_URL = "https://api.app.biggie.com.py/api/articles"


def test_biggie_skip_emits_offsets_when_count_present() -> None:
    scraper = _scraper(BiggieScraper, BIGGIE_BASE_URL, page_size=100)
    seed = scraper.seed_urls()[0]
    response = FakeResponse(json_body={"count": 350})

    urls = scraper.get_next_urls(seed, response)  # type: ignore[arg-type]

    assert urls == [scraper.build_url(skip) for skip in (100, 200, 300)]
    return


def test_biggie_skip_raises_when_count_missing() -> None:
    scraper = _scraper(BiggieScraper, BIGGIE_BASE_URL)
    seed = scraper.seed_urls()[0]
    response = FakeResponse(json_body={})

    with pytest.raises(ScraperError):
        scraper.get_next_urls(seed, response)  # type: ignore[arg-type]
    return


# LaNacion concurrency-offset paging — each full page emits `concurrency` more offsets.

LANACION_BASE_URL = (
    "https://www.lanacion.com.py/pf/api/v3/content/fetch/content-search-feed-full"
)


def test_lanacion_emits_concurrency_offsets_on_full_page() -> None:
    scraper = _scraper(LaNacionScraper, LANACION_BASE_URL, concurrency=5, page_size=100)
    seed = scraper.seed_urls()[0]
    response = FakeResponse(json_body={"content_elements": [{} for _ in range(100)]})

    urls = scraper.get_next_urls(seed, response)  # type: ignore[arg-type]

    assert len(urls) == 5
    offsets = []
    for url in urls:
        blob = json.loads(parse_qs(urlparse(url).query)["query"][0])
        offsets.append(int(blob["feedFrom"]))
    assert offsets == [100, 200, 300, 400, 500]
    return


def test_lanacion_empty_on_short_page() -> None:
    scraper = _scraper(LaNacionScraper, LANACION_BASE_URL, concurrency=5, page_size=100)
    seed = scraper.seed_urls()[0]
    response = FakeResponse(json_body={"content_elements": [{} for _ in range(50)]})

    assert scraper.get_next_urls(seed, response) == []  # type: ignore[arg-type]
    return


def test_lanacion_raises_on_malformed_seed() -> None:
    scraper = _scraper(LaNacionScraper, LANACION_BASE_URL)
    response = FakeResponse(json_body={"content_elements": [{} for _ in range(100)]})

    with pytest.raises((KeyError, IndexError)):
        scraper.get_next_urls(LANACION_BASE_URL, response)  # type: ignore[arg-type]
    return
