import json
from typing import Any

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpRequestBuilder, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for latribuna — Arc Publishing feed, open-ended offset pagination into bronze.api_snapshots."""

    bronze_model = ApiSnapshot
    SIZE = 100

    def build_url(
        self,
        offset: int | None = None,
        url: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> HttpRequest:
        query = json.dumps(
            {
                "query": "type:story",
                "offset": offset,
                "size": self.SIZE,
            }
        )
        return (
            HttpRequestBuilder()
            .set_url(url if url is not None else self.config.base_url)
            .set_headers(self.config.headers)
            .set_params(params if params is not None else {"query": query})
            .build()
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(0)]

    def get_next_urls(self, response: HttpResponse, soup: object = None) -> list[HttpRequest]:
        elements = response.json().get("content_elements", [])
        if len(elements) < self.SIZE:
            return []
        blob = json.loads(response.request.params.get("query", "{}"))
        next_offset = int(blob.get("offset", 0)) + self.SIZE
        return [self.build_url(next_offset)]
