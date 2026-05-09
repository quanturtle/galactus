import json
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot

ENDPOINT = "/pf/api/v3/content/fetch/sections-api"
SECTIONS = (
    "/nacionales",
    "/internacionales",
    "/politica",
    "/economia",
    "/deportes",
    "/policiales",
    "/espectaculos",
    "/opinion",
    "/ciencia",
    "/tecnologia",
    "/locales",
    "/sociedad",
)


class Scraper(BaseScraper):
    """Scraper for abc_color — Arc Publishing sections-api, per-section pagination into bronze.api_snapshots."""

    bronze_model = ApiSnapshot

    def _build_url(self, section: str, offset: int) -> str:
        query = json.dumps(
            {
                "section_id": section,
                "sort": "display_date:desc",
                "limit": str(self.options.page_size),
                "offset": str(offset),
            }
        )
        return urljoin(self.options.base_url, ENDPOINT) + "?" + urlencode({"query": query})

    def _current(self, url: str) -> tuple[str, int]:
        params = parse_qs(urlparse(url).query)
        blob = json.loads(params.get("query", ["{}"])[0])
        return blob["section_id"], int(blob.get("offset", "0"))

    def seed_urls(self) -> list[str]:
        return [self._build_url(section, 0) for section in SECTIONS]

    def discover_links(self, url: str, response: HttpResponse) -> list[str]:
        elements = response.json().get("content_elements", [])
        if len(elements) < self.options.page_size:
            return []
        section, offset = self._current(url)
        return [self._build_url(section, offset + self.options.page_size)]
