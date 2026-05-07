from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

from galactus.core.records import RawRecord
from galactus.core.types import SourceName
from galactus.infra.http import HttpxClient


class Scraper(ABC):
    """Strategy contract for fetching one source's raw records.

    Concrete scrapers receive their I/O dependencies via the constructor
    and yield RawRecords from fetch(). They never touch the database.
    """

    def __init__(
        self,
        *,
        source: SourceName,
        http: HttpxClient,
        options: dict[str, Any],
        concurrency: int = 1,
    ) -> None:
        self.source = source
        self.http = http
        self.options = options
        self.concurrency = concurrency

    @abstractmethod
    def fetch(self) -> AsyncIterator[RawRecord]:
        """Yield RawRecords for this source. Implementations should be async generators."""
        ...
