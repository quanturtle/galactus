from urllib.parse import urlencode

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for grutter — WooCommerce Store API, page-bounded pagination into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot

    def build_url(self, page: int) -> str:
        return f"{self.options.base_url}?{urlencode({'page': str(page)})}"

    def seed_urls(self) -> list[str]:
        return [self.build_url(1)]

    def get_next_urls(self, url: str, response: HttpResponse) -> list[str]:
        total = int(response.headers.get("x-wp-totalpages", "1"))
        return [self.build_url(page) for page in range(2, total + 1)]
