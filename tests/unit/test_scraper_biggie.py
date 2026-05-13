import pytest

from galactus.config import ExtractOptions
from galactus.core.errors import ScraperError
from galactus.extract.scrapers.supermercados.biggie import Scraper
from tests.unit.test_base_scraper import FakeDatabase, FakeHttpClient, FakeResponse


def make_biggie(page_size: int = 100) -> Scraper:
    options = ExtractOptions(
        base_url="https://api.app.biggie.com.py/api/articles",
        page_size=page_size,
    )
    return Scraper(
        source="biggie",
        http=FakeHttpClient(),  # type: ignore[arg-type]
        db=FakeDatabase(),  # type: ignore[arg-type]
        options=options,
        concurrency=1,
    )


def test_get_next_urls_emits_skips_when_count_present() -> None:
    scraper = make_biggie(page_size=100)
    seed = scraper.seed_urls()[0]
    response = FakeResponse(json_body={"count": 350})

    urls = scraper.get_next_urls(seed, response)  # type: ignore[arg-type]

    assert urls == [scraper.build_url(skip) for skip in (100, 200, 300)]
    return


def test_get_next_urls_raises_when_count_missing() -> None:
    scraper = make_biggie()
    seed = scraper.seed_urls()[0]
    response = FakeResponse(json_body={})

    with pytest.raises(ScraperError):
        scraper.get_next_urls(seed, response)  # type: ignore[arg-type]
    return
