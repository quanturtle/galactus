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
                "feedSize": str(self.options.page_size),
                "feedFrom": str(offset),
                "website": self.WEBSITE,
                "feedQuery": "type:story",
            }
        )
        return f"{self.options.base_url}?{urlencode({'query': query})}"

    def seed_urls(self) -> list[str]:
        return [self.build_url(0)]

    def get_next_urls(self, url: str, response: HttpResponse) -> list[str]:
        page_size = self.options.page_size
        elements = response.json().get("content_elements", [])
        if len(elements) < page_size:
            return []
        blob = json.loads(parse_qs(urlparse(url).query).get("query", ["{}"])[0])
        next_offset = int(blob.get("feedFrom", "0")) + page_size
        return [self.build_url(next_offset)]
