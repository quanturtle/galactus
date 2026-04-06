import httpx

from supermercados.scrapers._base import ApiScraper


class GrutterScraper(ApiScraper):
    source = "grutter"

    def _build_params(self, page_index: int) -> dict:
        return {"per_page": self.page_size, "page": page_index + 1}

    def _extract_total_pages(self, response: httpx.Response) -> int:
        return int(response.headers.get("X-WP-TotalPages", 1))
