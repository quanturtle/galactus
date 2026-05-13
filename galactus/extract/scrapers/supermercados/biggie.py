from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for biggie — products JSON API, offset-paginated into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot

    def build_url(self, skip: int) -> HttpRequest:
        return HttpRequest(
            url=self.config.base_url,
            headers=dict(self.config.headers),
            params={**self.config.params, "skip": str(skip)},
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(0)]

    def get_next_urls(self, response: HttpResponse) -> list[HttpRequest]:
        page_size = self.config.page_size
        body = response.json()
        if "count" not in body:
            raise ScraperError(f"biggie: missing 'count' in {response.url}")
        total = int(body["count"])
        return [self.build_url(skip) for skip in range(page_size, total, page_size)]
