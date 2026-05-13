import json
from urllib.parse import parse_qs, urlencode, urlparse

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for lanacion — Arc Publishing feed, open-ended offset pagination into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot
    WEBSITE = "lanacionpy"

    def build_url(self, offset: int) -> str:
        query = json.dumps(
            {
                "feedSize": str(self.config.page_size),
                "feedFrom": str(offset),
                "website": self.WEBSITE,
                "feedQuery": "type:story",
            }
        )
        return f"{self.config.base_url}?{urlencode({'query': query})}"

    def seed_urls(self) -> list[str]:
        return [self.build_url(0)]

    async def process_response(self, url: str, response: HttpResponse) -> list[str]:
        # skip overshoot pages from in-flight fetches past the natural end — don't persist empty bronze rows
        if not response.json().get("content_elements", []):
            return []
        return await super().process_response(url, response)

    def get_next_urls(self, url: str, response: HttpResponse) -> list[str]:
        page_size = self.config.page_size
        elements = response.json().get("content_elements", [])
        if len(elements) < page_size:
            return []
        blob = json.loads(parse_qs(urlparse(url).query)["query"][0])
        current = int(blob["feedFrom"])
        return [self.build_url(current + i * page_size) for i in range(1, self.concurrency + 1)]
