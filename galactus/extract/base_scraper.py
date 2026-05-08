from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from galactus.config import ExtractOptions
from galactus.core.records import RawRecord
from galactus.infra.http import HttpClient, HttpResponse


class BaseScraper(ABC):
    """Base class for all scrapers. Owns the BFS loop; subclasses provide three hooks.

    Concrete scrapers never override run() — they implement seeds(),
    extract_links(), and build_record() instead.
    """

    def __init__(
        self,
        source: str,
        http: HttpClient,
        options: ExtractOptions,
        concurrency: int = 1,
    ) -> None:
        self.source = source
        self.http = http
        self.options = options
        self.concurrency = concurrency

    @abstractmethod
    def seeds(self) -> list[str]:
        """Return the initial URLs to enqueue."""
        ...

    @abstractmethod
    def extract_links(self, url: str, response: HttpResponse) -> list[str]:
        """Return URLs discovered in response to continue the traversal."""
        ...

    @abstractmethod
    def build_record(self, url: str, response: HttpResponse) -> RawRecord:
        """Wrap the response into the appropriate snapshot record type."""
        ...

    async def run(self) -> AsyncIterator[RawRecord]:
        """BFS loop over seeds(). Calls extract_links and build_record per URL."""
        queue: list[str] = self.seeds()
        visited: set[str] = set()
        # placeholder — HTTP fetch + concurrency logic ported from v1 later
        return
        yield
