import json

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for lanacion — Arc Publishing feed, open-ended offset pagination into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot
    WEBSITE = "lanacionpy"

    def build_url(self, offset: int) -> HttpRequest:
        query = json.dumps(
            {
                "feedSize": str(self.config.page_size),
                "feedFrom": str(offset),
                "website": self.WEBSITE,
                "feedQuery": "type:story",
            }
        )
        return HttpRequest(
            url=self.config.base_url,
            headers=dict(self.config.headers),
            params={**self.config.params, "query": query},
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(0)]

    async def process_response(self, response: HttpResponse) -> list[HttpRequest]:
        # skip overshoot pages from in-flight fetches past the natural end — don't persist empty bronze rows
        if not response.json().get("content_elements", []):
            return []
        return await super().process_response(response)

    def get_next_urls(self, response: HttpResponse) -> list[HttpRequest]:
        page_size = self.config.page_size
        elements = response.json().get("content_elements", [])
        if len(elements) < page_size:
            return []
        blob = json.loads(response.request.params["query"])
        current = int(blob["feedFrom"])
        return [self.build_url(current + i * page_size) for i in range(1, self.concurrency + 1)]
