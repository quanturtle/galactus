import json
from urllib.parse import parse_qs, urlencode, urlparse

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for abc_color — Arc Publishing sections-api, per-section pagination into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot
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

    def build_url(self, section: str, offset: int) -> str:
        query = json.dumps(
            {
                "section_id": section,
                "sort": "display_date:desc",
                "limit": str(self.options.page_size),
                "offset": str(offset),
            }
        )
        return f"{self.options.base_url}?{urlencode({'query': query})}"

    def seed_urls(self) -> list[str]:
        return [self.build_url(section, 0) for section in self.SECTIONS]

    def get_next_urls(self, url: str, response: HttpResponse) -> list[str]:
        page_size = self.options.page_size
        elements = response.json().get("content_elements", [])
        if len(elements) < page_size:
            return []
        blob = json.loads(parse_qs(urlparse(url).query).get("query", ["{}"])[0])
        section, offset = blob["section_id"], int(blob.get("offset", "0"))
        return [self.build_url(section, offset + page_size)]
