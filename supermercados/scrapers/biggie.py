import math

import httpx

from supermercados.scrapers._base import ApiScraper


class BiggieScraper(ApiScraper):
    source = "biggie"

    def _build_params(self, page_index: int) -> dict:
        return {"take": self.page_size, "skip": page_index * self.page_size}

    def _extract_total_pages(self, response: httpx.Response) -> int:
        total = response.json()["count"]
        return math.ceil(total / self.page_size)
