from collections.abc import AsyncIterator

from galactus.core.records import RawRecord
from galactus.extract.base import Scraper


class BfsScraper(Scraper):
    """Generic BFS HTML crawler.

    Domain-specific subclasses provide a URL pattern and content-page predicate.
    Concrete crawl logic will be ported from v1's src/galactus/scrapers/bfs.py.
    """

    async def fetch(self) -> AsyncIterator[RawRecord]:
        # placeholder — concrete BFS logic ported from v1 later
        return
        yield  # pragma: no cover
