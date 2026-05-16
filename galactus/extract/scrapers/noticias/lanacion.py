import json

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for lanacion — Arc Publishing feed, open-ended offset pagination into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot
    WEBSITE = "lanacionpy"
    FEED_SIZE = 100
    # Arc's feed sits on Elasticsearch; ES rejects feedFrom+feedSize > index.max_result_window (default 10000).
    MAX_RESULT_WINDOW = 10000

    def build_url(self, offset: int) -> HttpRequest:
        query = json.dumps(
            {
                "feedSize": str(self.FEED_SIZE),
                "feedFrom": str(offset),
                "website": self.WEBSITE,
                "feedQuery": "type:story",
            }
        )
        return HttpRequest(
            url=self.config.base_url,
            headers=dict(self.config.headers),
            params={"query": query},
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(0)]

    async def process_response(self, response: HttpResponse) -> list[HttpRequest]:
        # non-200 bodies aren't JSON; bail before .json() crashes the run
        if response.status_code != 200:
            return []
        # parse the feed body once and hand the elements to get_next_urls via a
        # scratch attribute; httpx re-decodes on every .json() call. Race-free
        # because run() awaits each process_response before starting the next.
        elements = response.json().get("content_elements", [])
        # skip overshoot pages from in-flight fetches past the natural end — don't persist empty bronze rows
        if not elements:
            return []
        self._last_elements = elements
        return await super().process_response(response)

    def get_next_urls(self, response: HttpResponse) -> list[HttpRequest]:
        if len(self._last_elements) < self.FEED_SIZE:
            return []
        blob = json.loads(response.request.params["query"])
        current = int(blob["feedFrom"])
        # ES caps feedFrom+feedSize at MAX_RESULT_WINDOW; never queue a request that would 400.
        return [
            self.build_url(current + i * self.FEED_SIZE)
            for i in range(1, self.concurrency + 1)
            if current + i * self.FEED_SIZE + self.FEED_SIZE <= self.MAX_RESULT_WINDOW
        ]
