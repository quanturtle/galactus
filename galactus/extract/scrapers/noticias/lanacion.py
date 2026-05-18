import json
from typing import Any

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for lanacion — Arc Publishing feed, open-ended offset pagination into bronze.api_snapshots."""

    bronze_model = ApiSnapshot
    WEBSITE = "lanacionpy"
    FEED_SIZE = 100
    # Arc's feed sits on Elasticsearch; ES rejects feedFrom+feedSize > index.max_result_window (default 10000).
    MAX_RESULT_WINDOW = 10000

    def build_url(
        self,
        offset: int | None = None,
        url: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> HttpRequest:
        query = json.dumps(
            {
                "feedSize": str(self.FEED_SIZE),
                "feedFrom": str(offset),
                "website": self.WEBSITE,
                "feedQuery": "type:story",
            }
        )
        return HttpRequest(
            url=url if url is not None else self.config.base_url,
            headers=dict(self.config.headers),
            params=params if params is not None else {"query": query},
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(0)]

    async def process_response(self, response: HttpResponse) -> list[HttpRequest]:
        # non-200 bodies aren't JSON; bail before .json() crashes the run
        if response.status_code != 200:
            return []
        items = response.json().get("content_elements", [])
        # skip overshoot pages from in-flight fetches past the natural end — don't persist empty bronze rows
        if not items:
            return []
        # partial last page: persist, but don't queue more pages (get_next_urls' return is discarded)
        if len(items) < self.FEED_SIZE:
            await super().process_response(response)
            return []
        return await super().process_response(response)

    def get_next_urls(self, response: HttpResponse, soup: object = None) -> list[HttpRequest]:
        blob = json.loads(response.request.params["query"])
        current = int(blob["feedFrom"])
        # ES caps feedFrom+feedSize at MAX_RESULT_WINDOW; never queue a request that would 400.
        return [
            self.build_url(current + i * self.FEED_SIZE)
            for i in range(1, self.concurrency + 1)
            if current + i * self.FEED_SIZE + self.FEED_SIZE <= self.MAX_RESULT_WINDOW
        ]
