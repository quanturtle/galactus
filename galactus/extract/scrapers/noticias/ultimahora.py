from galactus.core.records import RawRecord
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse


class Scraper(BaseScraper):
    """Scraper for ultimahora.com — BFS crawl."""

    def seeds(self) -> list[str]:
        return [self.options["home_url"]]

    def extract_links(self, url: str, response: HttpResponse) -> list[str]:
        return []  # placeholder — parse <a href> links in follow-up

    def build_record(self, url: str, response: HttpResponse) -> RawRecord:
        raise NotImplementedError  # placeholder
