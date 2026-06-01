import json
from typing import Any

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpRequestBuilder, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for abc_color — Arc Publishing sections-api, per-section pagination into bronze.api_snapshots."""

    bronze_model = ApiSnapshot
    WEBSITE = "abc-color"
    LIMIT = 100
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

    def build_url(
        self,
        section: str | None = None,
        offset: int | None = None,
        url: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> HttpRequest:
        query = json.dumps(
            {
                "section_id": section,
                "sort": "display_date:desc",
                "limit": str(self.LIMIT),
                "offset": str(offset),
            }
        )
        return (
            HttpRequestBuilder()
            .set_url(url if url is not None else self.config.base_url)
            .set_headers(self.config.headers)
            .set_params(
                params if params is not None else {"_website": self.WEBSITE, "query": query}
            )
            .build()
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(section, 0) for section in self.SECTIONS]

    def get_next_urls(self, response: HttpResponse, soup: object = None) -> list[HttpRequest]:
        # the API returns HTTP 400 with a non-JSON body once offset crosses its hard ceiling;
        # treat any non-JSON response as end-of-feed for the section.
        try:
            elements = response.json().get("content_elements", [])
        except ValueError:
            return []
        if len(elements) < self.LIMIT:
            return []
        blob = json.loads(response.request.params.get("query", "{}"))
        section, offset = blob["section_id"], int(blob.get("offset", "0"))
        return [self.build_url(section, offset + self.LIMIT)]
