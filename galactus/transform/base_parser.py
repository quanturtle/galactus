import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup

from galactus.core.errors import ParserError
from galactus.core.records import ParsedRecord, RawRecord
from galactus.infra.db import Database
from galactus.transform.html_cleaner import HtmlParser, decompress

logger = logging.getLogger(__name__)
UTC = timezone.utc


class BaseParser(ABC):
    """Base class for all parsers. Owns the bronze→silver lifecycle; subclasses provide one hook.

    HTML sources call self.clean() and self.soup() inside parse().
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

    @abstractmethod
    def parse(self, record: RawRecord) -> ParsedRecord | list[ParsedRecord]:
        """Parse one bronze record into one or many silver records."""
        ...

    def clean(self, html_bytes: bytes) -> str:
        """Decompress and clean a bronze html blob. For use inside parse()."""
        return self.html_parser.clean(decompress(html_bytes))

    def soup(self, html_bytes: bytes) -> BeautifulSoup:
        """Decompress a bronze html blob and return a filtered BeautifulSoup tree."""
        return self.html_parser.parse(decompress(html_bytes))

    async def run(self) -> None:
        batch: list[ParsedRecord] = []
        ids: list[int] = []

        async for record in self.db.load_unparsed(self.source, self.bronze_table):
            try:
                result = self.parse(record)
            except ParserError as exc:
                logger.warning("parse failed for %s %s: %s", self.source, record.source_url, exc)
                continue

            parsed = result if isinstance(result, list) else [result]
            batch.extend(parsed)
            if record.bronze_id is not None:
                ids.append(record.bronze_id)

            if len(batch) >= self.batch_size:
                await self.db.upsert(batch, self.silver_table)
                await self.db.mark_parsed(ids, self.bronze_table)
                batch.clear()
                ids.clear()

        # flush remainder
        if batch:
            await self.db.upsert(batch, self.silver_table)
            await self.db.mark_parsed(ids, self.bronze_table)

        return
