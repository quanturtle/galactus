from urllib.parse import urlencode

from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for megacadena — WordPress REST, page-bounded pagination into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot

    def build_url(self, page: int) -> str:
        return f"{self.config.base_url}?{urlencode({'page': str(page)})}"

    def seed_urls(self) -> list[str]:
        return [self.build_url(1)]

    def get_next_urls(self, url: str, response: HttpResponse) -> list[str]:
        header = response.headers.get("x-wp-totalpages")
        if header is None:
            raise ScraperError(f"megacadena: missing x-wp-totalpages header at {url}")
        total = int(header)
        return [self.build_url(page) for page in range(2, total + 1)]
