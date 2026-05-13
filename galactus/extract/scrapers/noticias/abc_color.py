import json

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
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

    def build_url(self, section: str, offset: int) -> HttpRequest:
        query = json.dumps(
            {
                "section_id": section,
                "sort": "display_date:desc",
                "limit": str(self.config.page_size),
                "offset": str(offset),
            }
        )
        return HttpRequest(
            url=self.config.base_url,
            headers=dict(self.config.headers),
            params={**self.config.params, "query": query},
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(section, 0) for section in self.SECTIONS]

    def get_next_urls(self, response: HttpResponse) -> list[HttpRequest]:
        page_size = self.config.page_size
        elements = response.json().get("content_elements", [])
        if len(elements) < page_size:
            return []
        blob = json.loads(response.request.params.get("query", "{}"))
        section, offset = blob["section_id"], int(blob.get("offset", "0"))
        return [self.build_url(section, offset + page_size)]
