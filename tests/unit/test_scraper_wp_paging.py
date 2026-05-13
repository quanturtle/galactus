import pytest

from galactus.config import ExtractOptions
from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from galactus.extract.scrapers.noticias.hoy import Scraper as HoyScraper
from galactus.extract.scrapers.noticias.megacadena import Scraper as MegacadenaScraper
from galactus.extract.scrapers.supermercados.grutter import Scraper as GrutterScraper
from tests.unit.test_base_scraper import FakeDatabase, FakeHttpClient, FakeResponse


def make_scraper(cls: type[BaseScraper]) -> BaseScraper:
    options = ExtractOptions(base_url="https://example.test/wp-json/wp/v2/posts")
    return cls(
        source="testsrc",
        http=FakeHttpClient(),  # type: ignore[arg-type]
        db=FakeDatabase(),  # type: ignore[arg-type]
        options=options,
        concurrency=1,
    )


@pytest.mark.parametrize("cls", [HoyScraper, MegacadenaScraper, GrutterScraper])
def test_get_next_urls_emits_pages_when_header_present(cls: type[BaseScraper]) -> None:
    scraper = make_scraper(cls)
    seed = scraper.seed_urls()[0]
    response = FakeResponse(headers={"x-wp-totalpages": "5"})

    urls = scraper.get_next_urls(seed, response)  # type: ignore[arg-type]

    assert urls == [scraper.build_url(p) for p in range(2, 6)]
    return


@pytest.mark.parametrize("cls", [HoyScraper, MegacadenaScraper, GrutterScraper])
def test_get_next_urls_raises_when_header_missing(cls: type[BaseScraper]) -> None:
    scraper = make_scraper(cls)
    seed = scraper.seed_urls()[0]
    response = FakeResponse(headers={})

    with pytest.raises(ScraperError):
        scraper.get_next_urls(seed, response)  # type: ignore[arg-type]
    return
