import json
from urllib.parse import parse_qs, urlencode, urlparse

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for latribuna — Arc Publishing feed, open-ended offset pagination into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot

    def build_url(self, offset: int) -> str:
        query = json.dumps(
            {
                "query": "type:story",
                "offset": offset,
                "size": self.options.page_size,
            }
        )
        return f"{self.options.base_url}?{urlencode({'query': query})}"

    def seeds(self) -> list[str]:
        return [self.build_url(0)]

    def next_urls(self, url: str, response: HttpResponse) -> list[str]:
        page_size = self.options.page_size
        elements = response.json().get("content_elements", [])
        if len(elements) < page_size:
            return []
        blob = json.loads(parse_qs(urlparse(url).query).get("query", ["{}"])[0])
        next_offset = int(blob.get("offset", 0)) + page_size
        return [self.build_url(next_offset)]
