from sql.base import Base
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse


class Scraper(BaseScraper):
    """Scraper for stock.com.py — paginated JSON API."""

    def seeds(self) -> list[str]:
        return [self.options.base_url]  # placeholder

    def extract_links(self, url: str, response: HttpResponse) -> list[str]:
        return []  # placeholder — parse next-page URL from JSON body

    def build_record(self, url: str, response: HttpResponse) -> Base:
        raise NotImplementedError  # placeholder
