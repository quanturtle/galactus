from urllib.parse import parse_qs, urlencode, urlparse

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for grutter — WooCommerce Store API, page-bounded pagination into bronze.api_snapshots."""

    bronze_model = ApiSnapshot

    def _build_url(self, page: int) -> str:
        params = {"per_page": str(self.options.page_size), "page": str(page)}
        return self.options.base_url + "?" + urlencode(params)

    def _current_page(self, url: str) -> int:
        params = parse_qs(urlparse(url).query)
        return int(params.get("page", ["1"])[0])

    def seed_urls(self) -> list[str]:
        return [self._build_url(1)]

    def normalize_url(self, href: str, page_url: str) -> str | None:
        # discover_links emits canonical absolute API URLs; the base normalize_url would
        # force a www. prefix this host does not have, breaking should_enqueue + fetch.
        return href

    def discover_links(self, url: str, response: HttpResponse) -> list[str]:
        # WooCommerce reports total pages in a response header
        total = int(response.headers.get("x-wp-totalpages", "1"))
        page = self._current_page(url)
        if page >= total:
            return []
        return [self._build_url(page + 1)]
