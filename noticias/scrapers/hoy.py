import httpx

from noticias.scrapers._base import ApiScraper


class HoyScraper(ApiScraper):
    """Hoy scraper — WordPress REST API with page-based pagination."""

    source = "hoy"

    def _build_params(self, page_index: int) -> dict:
        return {"per_page": self.page_size, "page": page_index + 1, "_embed": "true"}

    def _extract_total_pages(self, response: httpx.Response) -> int:
        return int(response.headers.get("X-WP-TotalPages", 1))
