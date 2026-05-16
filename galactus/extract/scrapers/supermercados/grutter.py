from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for grutter — WooCommerce Store API, page-bounded pagination into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot
    PER_PAGE = 100

    def build_url(self, page: int) -> HttpRequest:
        return HttpRequest(
            url=self.config.base_url,
            headers=dict(self.config.headers),
            params={
                "per_page": str(self.PER_PAGE),
                "orderby": "date",
                "order": "desc",
                "page": str(page),
            },
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(1)]

    def get_next_urls(self, response: HttpResponse) -> list[HttpRequest]:
        # fan out the full page list only from the seed; later responses already
        # carry pages enqueued by the seed, so re-emitting them just wastes hashing.
        if response.request.params.get("page") != "1":
            return []
        header = response.headers.get("x-wp-totalpages")
        if header is None:
            raise ScraperError(f"grutter: missing x-wp-totalpages header at {response.url}")
        total = int(header)
        return [self.build_url(page) for page in range(2, total + 1)]
