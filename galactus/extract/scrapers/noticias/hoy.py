from typing import Any

from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpRequestBuilder, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for hoy — WordPress REST, page-bounded pagination into bronze.api_snapshots."""

    bronze_model = ApiSnapshot
    PER_PAGE = 100

    def build_url(
        self,
        page: int | None = None,
        url: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> HttpRequest:
        return (
            HttpRequestBuilder()
            .set_url(url if url is not None else self.config.base_url)
            .set_headers(self.config.headers)
            .set_params(
                params
                if params is not None
                else {
                    "per_page": str(self.PER_PAGE),
                    "_embed": "true",
                    "orderby": "date",
                    "order": "desc",
                    "page": str(page),
                }
            )
            .build()
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(1)]

    def get_next_urls(self, response: HttpResponse, soup: object = None) -> list[HttpRequest]:
        # fan out the full page list only from the seed; later responses already
        # carry pages enqueued by the seed, so re-emitting them just wastes hashing.
        if response.request.params.get("page") != "1":
            return []
        header = response.headers.get("x-wp-totalpages")
        if header is None:
            raise ScraperError(f"hoy: missing x-wp-totalpages header at {response.url}")
        total = int(header)
        return [self.build_url(page) for page in range(2, total + 1)]
