from sqlmodel import SQLModel
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse


class Scraper(BaseScraper):
    """Scraper for elnacional.com.py — BFS crawl."""

    def seeds(self) -> list[str]:
        return [self.options.base_url]

    def extract_links(self, url: str, response: HttpResponse) -> list[str]:
        return []  # placeholder — parse <a href> links in follow-up

    def build_record(self, url: str, response: HttpResponse) -> SQLModel:
        raise NotImplementedError  # placeholder
