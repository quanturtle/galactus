import json
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot

ENDPOINT = "/pf/api/v3/content/fetch/story-feed-query"


class Scraper(BaseScraper):
    """Scraper for latribuna — Arc Publishing feed, open-ended pagination into bronze.api_snapshots."""

    bronze_model = ApiSnapshot

    def _build_url(self, offset: int) -> str:
        query = json.dumps(
            {
                "query": "type:story",
                "offset": offset,
                "size": self.options.page_size,
            }
        )
        return urljoin(self.options.base_url, ENDPOINT) + "?" + urlencode({"query": query})

    def seed_urls(self) -> list[str]:
        return [self._build_url(0)]

    def next_urls(self, url: str, response: HttpResponse) -> list[str]:
        elements = response.json().get("content_elements", [])
        if len(elements) < self.options.page_size:
            return []
        blob = json.loads(parse_qs(urlparse(url).query).get("query", ["{}"])[0])
        next_offset = int(blob.get("offset", 0)) + self.options.page_size
        return [self._build_url(next_offset)]
