from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any, ClassVar

from galactus.config import TransformConfig
from galactus.core.errors import ParserError
from galactus.infra.db import Database
from galactus.transform.html_parser import HtmlParser
from sql.base import Base


class BaseParser(ABC):
    """Template Method base for all parsers.

    run() owns the bronze->silver lifecycle: stream unparsed bronze rows,
    decode each, build silver entities, batch-upsert into silver, then
    mark bronze rows parsed. Concrete parsers override hooks; both
    decode() and build_entities() are abstract.
    """

    bronze_model: ClassVar[type[Base]]
    silver_model: ClassVar[type[Base]]
    conflict_columns: ClassVar[tuple[str, ...]] = ("source", "source_url")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for attr in ("bronze_model", "silver_model"):
            if not hasattr(cls, attr):
                raise ParserError(f"{cls.__name__} must define class variable '{attr}'")

    def __init__(
        self,
        source: str,
        db: Database,
        bronze_table: str,
        silver_table: str,
        config: TransformConfig,
    ) -> None:
        self.source = source
        self.db = db
        self.bronze_table = bronze_table
        self.silver_table = silver_table
        self.config = config
        self.options = config.options
        self.batch_size = config.options.batch_size
        self.html_parser = self._make_html_parser(config.options)

    # hook: override to provide code-level blocklist defaults per parser
    def _make_html_parser(self, options: Any) -> HtmlParser:
        return HtmlParser(
            {
                "blocklist_tags": options.blocklist_tags,
                "blocklist_attributes": options.blocklist_attributes,
            }
        )

    # 1. load_records — stream of unparsed bronze rows
    def load_records(self) -> AsyncIterator[Base]:
        return self.db.load_unparsed(self.bronze_model, self.source)

    # 2. decode — bronze row -> parsed payload (BeautifulSoup for HTML, dict for JSON)
    @abstractmethod
    def decode(self, record: Base) -> Any: ...

    # 3. build_entities — bronze row + decoded payload -> silver records
    @abstractmethod
    def build_entities(self, record: Base, decoded: Any) -> list[Base]: ...

    async def run(self) -> None:
        """Lifecycle: stream unparsed bronze; decode + build silver; upsert in batches; mark parsed."""
        # init batch buffers
        bronze_ids: list[int] = []
        silver_batch: list[Base] = []

        # stream and accumulate
        async for record in self.load_records():
            decoded = self.decode(record)
            entities = self.build_entities(record, decoded)
            silver_batch.extend(entities)
            bronze_ids.append(record.bronze_id)

            # flush when batch is full
            if len(bronze_ids) >= self.batch_size:
                await self._flush(silver_batch, bronze_ids)
                silver_batch = []
                bronze_ids = []

        # flush trailing partial batch
        if bronze_ids:
            await self._flush(silver_batch, bronze_ids)

        return

    async def _flush(self, silver: list[Base], bronze_ids: list[int]) -> None:
        if silver:
            await self.db.upsert(
                silver, model=self.silver_model, conflict_columns=self.conflict_columns
            )
        await self.db.mark_parsed(self.bronze_model, bronze_ids)
        return
