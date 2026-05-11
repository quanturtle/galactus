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
        # stop once the next offset would be past the total the API reports
        total = int(response.json().get("count", 0))
        next_skip = query_int(url, "skip", 0) + self.options.page_size
        if next_skip >= total:
            return []
        return [self._build_url(next_skip)]
