import json
from urllib.parse import parse_qs, urlparse

import pytest

from galactus.config import ExtractOptions
from galactus.extract.scrapers.noticias.lanacion import Scraper
from tests.unit.test_base_scraper import FakeDatabase, FakeHttpClient, FakeResponse


def make_lanacion(concurrency: int = 5, page_size: int = 100) -> Scraper:
    options = ExtractOptions(
        base_url="https://www.lanacion.com.py/pf/api/v3/content/fetch/content-search-feed-full",
        page_size=page_size,
    )
    return Scraper(
        source="lanacion",
        http=FakeHttpClient(),  # type: ignore[arg-type]
        db=FakeDatabase(),  # type: ignore[arg-type]
        options=options,
        concurrency=concurrency,
    )


def test_get_next_urls_emits_concurrency_offsets_on_full_page() -> None:
    scraper = make_lanacion(concurrency=5, page_size=100)
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


def test_get_next_urls_empty_on_short_page() -> None:
    scraper = make_lanacion(concurrency=5, page_size=100)
    seed = scraper.seed_urls()[0]
    response = FakeResponse(json_body={"content_elements": [{} for _ in range(50)]})

    assert scraper.get_next_urls(seed, response) == []  # type: ignore[arg-type]
    return


def test_get_next_urls_raises_on_malformed_seed() -> None:
    scraper = make_lanacion()
    response = FakeResponse(json_body={"content_elements": [{} for _ in range(100)]})

    with pytest.raises((KeyError, IndexError)):
        scraper.get_next_urls("https://www.lanacion.com.py/pf/api/v3/content/fetch/content-search-feed-full", response)  # type: ignore[arg-type]
    return
