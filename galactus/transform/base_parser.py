from abc import ABC, abstractmethod
from typing import Any, ClassVar

from galactus.config import TransformConfig
from galactus.core.errors import DatabaseError, ParserError
from galactus.infra.db import Database
from galactus.transform.html_parser import HtmlParser
from sql.base import Base


class BaseParser(ABC):
    """Template Method base for all parsers.

    run() owns the bronze->silver lifecycle: load the bronze rows for the
    source that no silver row references yet, decode each, build silver
    entities, insert them all in one transaction. No dedup here — one silver
    row per (entity, bronze sighting); the gold layer collapses across
    sightings. A bronze row counts as parsed once any silver row carries its
    (source, bronze_id), so a re-run skips bronze rows whose silver already
    committed. Concrete parsers override hooks; both decode() and
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
        self.html_parser = self._make_html_parser(config.options)

    # hook: override to provide code-level blocklist defaults per parser
    def _make_html_parser(self, options: Any) -> HtmlParser:
        return HtmlParser(
            {
                "blocklist_tags": options.blocklist_tags,
                "blocklist_attributes": options.blocklist_attributes,
            }
        )

    # 1. load_records — all bronze rows for this source not yet in silver
    async def load_records(self) -> list[Base]:
        return await self.db.load_unparsed(self.bronze_model, self.silver_model, self.source)

    # 2. decode — bronze row -> parsed payload (BeautifulSoup for HTML, dict for JSON)
    @abstractmethod
    def decode(self, record: Base) -> Any: ...

    # 3. build_entities — bronze row + decoded payload -> silver records
    @abstractmethod
    def build_entities(self, record: Base, decoded: Any) -> list[Base]: ...

    # 4. _build_silver — decode + build every bronze row, stamping its provenance onto each entity
    def _build_silver(self, records: list[Base]) -> list[Base]:
        silver: list[Base] = []
        for record in records:
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
            silver.extend(entities)
        return silver

    async def run(self) -> None:
        """Lifecycle: load all unparsed bronze for source; decode + build silver; insert."""
        try:
            records = await self.load_records()
            silver = self._build_silver(records)
            await self.db.insert(
                silver,
                model=self.silver_model,
                exclude_columns=self.exclude_columns,
            )
        except DatabaseError as exc:
            raise ParserError(f"source {self.source!r}: bronze→silver failed") from exc
        return
