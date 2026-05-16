from typing import Any

from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for biggie — products JSON API, offset-paginated into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot
    TAKE = 50

    def build_url(
        self,
        skip: int | None = None,
        url: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> HttpRequest:
        return HttpRequest(
            url=url if url is not None else self.config.base_url,
            headers=dict(self.config.headers),
            params=params if params is not None else {"take": str(self.TAKE), "skip": str(skip)},
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(0)]

    def get_next_urls(self, response: HttpResponse) -> list[HttpRequest]:
        # fan out the full offset list only from the seed; later responses already
        # carry offsets enqueued by the seed, so re-emitting them just wastes hashing.
        if response.request.params.get("skip") != "0":
            return []
        body = response.json()
        if "count" not in body:
            raise ScraperError(f"biggie: missing 'count' in {response.url}")
        total = int(body["count"])
        return [self.build_url(skip) for skip in range(self.TAKE, total, self.TAKE)]
