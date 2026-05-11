from urllib.parse import urlencode, urljoin

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot

ENDPOINT = "/wp-json/wp/v2/posts"


class Scraper(BaseScraper):
    """Scraper for megacadena — WordPress REST, page-bounded pagination into bronze.api_snapshots."""

    bronze_model = ApiSnapshot

    def _build_url(self, page: int) -> str:
        params = {
            "per_page": str(self.options.page_size),
            "page": str(page),
            "_embed": "true",
        }
        return urljoin(self.options.base_url, ENDPOINT) + "?" + urlencode(params)

    def seed_urls(self) -> list[str]:
        return [self._build_url(1)]

    def next_urls(self, url: str, response: HttpResponse) -> list[str]:
        if url not in self._seeds:
            return []
        total = int(response.headers.get("x-wp-totalpages", "1"))
        return [self._build_url(page) for page in range(2, total + 1)]
