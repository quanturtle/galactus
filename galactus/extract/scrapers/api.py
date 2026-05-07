from collections.abc import AsyncIterator

from galactus.core.records import RawRecord
from galactus.extract.base import Scraper


class ApiScraper(Scraper):
    """Generic paginated-JSON scraper.

    Domain-specific subclasses customize URL construction and stop conditions.
    The skeleton implementation does nothing yet — subclasses or the generic
    flow should be filled in once the domain model is ported.
    """

    async def fetch(self) -> AsyncIterator[RawRecord]:
        # placeholder — concrete pagination logic ported from v1 later
        return
        yield  # pragma: no cover
