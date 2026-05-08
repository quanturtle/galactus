from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot

ENDPOINT = "/wp-json/wp/v2/posts"


class Scraper(BaseScraper):
    """Scraper for hoy — WordPress REST, page-bounded pagination into bronze.api_snapshots."""

    model = ApiSnapshot

    def _build_url(self, page: int) -> str:
        params = {
            "per_page": str(self.options.page_size),
            "page": str(page),
            "_embed": "true",
        }
        return urljoin(self.options.base_url, ENDPOINT) + "?" + urlencode(params)

    def _current_page(self, url: str) -> int:
        params = parse_qs(urlparse(url).query)
        return int(params.get("page", ["1"])[0])

    def seed_urls(self) -> list[str]:
        return [self._build_url(1)]

    def discover_links(self, url: str, response: HttpResponse) -> list[str]:
        total = int(response.headers.get("x-wp-totalpages", "1"))
        page = self._current_page(url)
        if page >= total:
            return []
        return [self._build_url(page + 1)]
