from urllib.parse import urlencode

from galactus.extract.base_scraper import BaseScraper, query_int
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for biggie — products JSON API, offset-paginated into bronze.api_snapshots."""

    bronze_model = ApiSnapshot

    def _build_url(self, skip: int) -> str:
        params = {"take": str(self.options.page_size), "skip": str(skip)}
        return self.options.base_url + "?" + urlencode(params)

    def seed_urls(self) -> list[str]:
        return [self._build_url(0)]

    def next_urls(self, url: str, response: HttpResponse) -> list[str]:
        if query_int(url, "skip", 0) != 0:
            return []
        page_size = self.options.page_size
        total = int(response.json().get("count", 0))
        return [self._build_url(skip) for skip in range(page_size, total, page_size)]
