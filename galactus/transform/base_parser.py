from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any, ClassVar

from galactus.config import TransformConfig
from galactus.core.errors import DatabaseError, ParserError
from galactus.infra.db import Database
from galactus.transform.html_parser import HtmlParser
from sql.base import Base


class BaseParser(ABC):
    """Template Method base for all parsers.

    run() owns the bronze->silver lifecycle: full-rescan bronze rows for the
    source, decode each, build silver entities, batch-upsert into silver.
    Idempotent re-scan: silver upsert on (source, source_url) is the dedup;
    no per-row tracking on bronze. Concrete parsers override hooks; both
    decode() and build_entities() are abstract.
    """

    bronze_model: ClassVar[type[Base]]
    silver_model: ClassVar[type[Base]]
    conflict_columns: ClassVar[tuple[str, ...]] = ("source", "source_url")
    # server-managed silver columns to drop from row dicts (id from the sequence;
    # created_at/updated_at are stamped from the bronze row in run(), not __init__)
    exclude_columns: ClassVar[tuple[str, ...]] = ("id",)

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

    # 1. load_records — full-rescan stream of bronze rows for this source
    def load_records(self) -> AsyncIterator[Base]:
        return self.db.load_for_source(self.bronze_model, self.source)

    # 2. decode — bronze row -> parsed payload (BeautifulSoup for HTML, dict for JSON)
    @abstractmethod
    def decode(self, record: Base) -> Any: ...

    # 3. build_entities — bronze row + decoded payload -> silver records
    @abstractmethod
    def build_entities(self, record: Base, decoded: Any) -> list[Base]: ...

    async def run(self) -> None:
        """Lifecycle: full-rescan bronze for source; decode + build silver; upsert in batches."""
        # init batch buffer
        silver_batch: list[Base] = []

        # stream bronze and accumulate silver; a DatabaseError from load/flush becomes ParserError
        try:
            async for record in self.load_records():
                # decode + build silver; surface subclass failures as ParserError
                try:
                    decoded = self.decode(record)
                    entities = self.build_entities(record, decoded)
                except ParserError:
                    raise
                except Exception as exc:
                    raise ParserError(
                        f"source {self.source!r}: bronze_id {record.bronze_id} decode/build failed"
                    ) from exc

                # stamp bronze provenance: silver created_at/updated_at mirror the bronze row's
                # timestamp; db.upsert resolves conflicts to the min/max across all bronze sightings
                for entity in entities:
                    entity.created_at = record.created_at
                    entity.updated_at = record.created_at
                silver_batch.extend(entities)

                # flush when batch is full
                if len(silver_batch) >= self.batch_size:
                    await self._flush(silver_batch)
                    silver_batch = []

            # flush trailing partial batch
            if silver_batch:
                await self._flush(silver_batch)
        except DatabaseError as exc:
            raise ParserError(f"source {self.source!r}: bronze→silver failed") from exc

        return

    async def _flush(self, silver: list[Base]) -> None:
        if silver:
            await self.db.upsert(
                silver,
                model=self.silver_model,
                conflict_columns=self.conflict_columns,
                exclude_columns=self.exclude_columns,
            )
        return
