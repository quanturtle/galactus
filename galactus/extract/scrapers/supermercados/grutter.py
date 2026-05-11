from urllib.parse import urlencode

from galactus.extract.base_scraper import BaseScraper, query_int
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for grutter — WooCommerce Store API, page-bounded pagination into bronze.api_snapshots."""

    bronze_model = ApiSnapshot

    def _build_url(self, page: int) -> str:
        params = {"per_page": str(self.options.page_size), "page": str(page)}
        return self.options.base_url + "?" + urlencode(params)

    def seed_urls(self) -> list[str]:
        return [self._build_url(1)]

    def next_urls(self, url: str, response: HttpResponse) -> list[str]:
        # WooCommerce reports total pages in a response header
        total = int(response.headers.get("x-wp-totalpages", "1"))
        page = query_int(url, "page", 1)
        if page >= total:
            return []
        return [self._build_url(page + 1)]
