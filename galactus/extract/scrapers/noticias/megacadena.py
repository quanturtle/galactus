from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for megacadena — WordPress REST, page-bounded pagination into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot
    PER_PAGE = 100

    def build_url(self, page: int) -> HttpRequest:
        return HttpRequest(
            url=self.config.base_url,
            headers=dict(self.config.headers),
            params={
                "per_page": str(self.PER_PAGE),
                "_embed": "true",
                "orderby": "date",
                "order": "desc",
                "page": str(page),
            },
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(1)]

    def get_next_urls(self, response: HttpResponse) -> list[HttpRequest]:
        header = response.headers.get("x-wp-totalpages")
        if header is None:
            raise ScraperError(f"megacadena: missing x-wp-totalpages header at {response.url}")
        total = int(header)
        return [self.build_url(page) for page in range(2, total + 1)]
