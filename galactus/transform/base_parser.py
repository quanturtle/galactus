from abc import ABC, abstractmethod
from typing import Any

from bs4 import BeautifulSoup

from galactus.infra.db import Database
from galactus.transform.html_parser import HtmlParser, decompress
from sql.base import Base


class BaseParser(ABC):
    """Base class for all parsers. Owns the bronze->silver lifecycle.

    Subclasses implement parse_batch(); the concrete run() loads unparsed
    records, accumulates them into batches, calls parse_batch(), upserts,
    and marks them parsed.

    HTML sources call self.clean() and self.soup() inside parse_batch().
    API sources call decompress() + json.loads() directly.
    """

    def __init__(
        self,
        source: str,
        db: Database,
        bronze_table: str,
        silver_table: str,
        options: dict[str, Any],
        batch_size: int = 100,
    ) -> None:
        self.source = source
        self.db = db
        self.bronze_table = bronze_table
        self.silver_table = silver_table
        self.batch_size = batch_size
        self.html_parser = HtmlParser(options)

    def clean(self, html_bytes: bytes) -> str:
        """Decompress and clean a bronze html blob. For use inside parse_batch()."""
        return self.html_parser.clean(decompress(html_bytes))

    def soup(self, html_bytes: bytes) -> BeautifulSoup:
        """Decompress a bronze html blob and return a filtered BeautifulSoup tree."""
        return self.html_parser.parse(decompress(html_bytes))

    @abstractmethod
    def parse_batch(self, records: list[Base]) -> list[Base]:
        """Parse a batch of bronze records into silver records."""
        ...

    async def run(self) -> None:
        """Lifecycle: load unparsed, parse in batches, upsert, mark parsed."""
        # placeholder — batch loop ported from v1 later
        raise NotImplementedError
