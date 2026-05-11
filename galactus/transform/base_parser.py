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

    run() owns the bronze->silver lifecycle: stream the bronze rows for the
    source that no silver row references yet, decode each, build silver
    entities, batch-insert them. No dedup here — one silver row per (entity,
    bronze sighting); the gold layer collapses across sightings. A bronze row
    counts as parsed once any silver row carries its (source, bronze_id), and
    each flush is one transaction over whole bronze rows, so a re-run skips
    already-committed work. Concrete parsers override hooks; both decode() and
    build_entities() are abstract.
    """

    bronze_model: ClassVar[type[Base]]
    silver_model: ClassVar[type[Base]]
    # server-managed silver column dropped from row dicts; bronze_id and created_at
    # are stamped from the bronze row in run(), so they stay in the dict
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

    # 1. load_records — stream of bronze rows for this source not yet in silver
    def load_records(self) -> AsyncIterator[Base]:
        return self.db.load_unparsed(self.bronze_model, self.silver_model, self.source)

    # 2. decode — bronze row -> parsed payload (BeautifulSoup for HTML, dict for JSON)
    @abstractmethod
    def decode(self, record: Base) -> Any: ...

    # 3. build_entities — bronze row + decoded payload -> silver records
    @abstractmethod
    def build_entities(self, record: Base, decoded: Any) -> list[Base]: ...

    async def run(self) -> None:
        """Lifecycle: stream unparsed bronze for source; decode + build silver; insert in batches."""
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

                # stamp bronze provenance: which bronze row this came from, and its snapshot time
                for entity in entities:
                    entity.bronze_id = record.bronze_id
                    entity.created_at = record.created_at
                silver_batch.extend(entities)

                # flush once the buffer holds a full batch (always on a bronze-row boundary)
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
            await self.db.insert(
                silver,
                model=self.silver_model,
                exclude_columns=self.exclude_columns,
            )
        return
