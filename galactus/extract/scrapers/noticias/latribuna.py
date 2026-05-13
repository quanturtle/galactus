import json

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for latribuna — Arc Publishing feed, open-ended offset pagination into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot

    def build_url(self, offset: int) -> HttpRequest:
        query = json.dumps(
            {
                "query": "type:story",
                "offset": offset,
                "size": self.config.page_size,
            }
        )
        return HttpRequest(
            url=self.config.base_url,
            headers=dict(self.config.headers),
            params={**self.config.params, "query": query},
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(0)]

    def get_next_urls(self, response: HttpResponse) -> list[HttpRequest]:
        page_size = self.config.page_size
        elements = response.json().get("content_elements", [])
        if len(elements) < page_size:
            return []
        blob = json.loads(response.request.params.get("query", "{}"))
        next_offset = int(blob.get("offset", 0)) + page_size
        return [self.build_url(next_offset)]
