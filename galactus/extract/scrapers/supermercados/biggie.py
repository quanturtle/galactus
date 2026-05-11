from urllib.parse import parse_qs, urlencode, urlparse

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for biggie — products JSON API, offset-paginated into bronze.api_snapshots."""

    bronze_model = ApiSnapshot

    def _build_url(self, skip: int) -> str:
        params = {"take": str(self.options.page_size), "skip": str(skip)}
        return self.options.base_url + "?" + urlencode(params)

    def _current_skip(self, url: str) -> int:
        params = parse_qs(urlparse(url).query)
        return int(params.get("skip", ["0"])[0])

    def seed_urls(self) -> list[str]:
        return [self._build_url(0)]

    def normalize_url(self, href: str, page_url: str) -> str | None:
        # discover_links emits canonical absolute API URLs; the base normalize_url would
        # force a www. prefix this host does not have, breaking should_enqueue + fetch.
        return href

    def discover_links(self, url: str, response: HttpResponse) -> list[str]:
        # stop once the next offset would be past the total the API reports
        total = int(response.json().get("count", 0))
        next_skip = self._current_skip(url) + self.options.page_size
        if next_skip >= total:
            return []
        return [self._build_url(next_skip)]
