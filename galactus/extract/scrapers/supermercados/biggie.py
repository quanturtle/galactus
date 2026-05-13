from urllib.parse import urlencode

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for biggie — products JSON API, offset-paginated into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot

    def build_url(self, skip: int) -> str:
        return f"{self.options.base_url}?{urlencode({'skip': str(skip)})}"

    def seed_urls(self) -> list[str]:
        return [self.build_url(0)]

    def get_next_urls(self, url: str, response: HttpResponse) -> list[str]:
        page_size = self.options.page_size
        total = int(response.json().get("count", 0))
        return [self.build_url(skip) for skip in range(page_size, total, page_size)]
