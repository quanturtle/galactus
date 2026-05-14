import json
from abc import ABC, abstractmethod
from typing import Any, ClassVar

from galactus.config import TransformConfig
from galactus.core.errors import DatabaseError, ParserError
from galactus.infra.db import Database
from galactus.transform.html_parser import HtmlParser, decompress
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.base import Base


class BaseParser(ABC):
    """Template Method base for all parsers.

    run() owns the bronze->silver lifecycle: load the bronze rows for the
    source that no silver row references yet, decode each, build silver
    entities, stamp provenance, insert them all in one transaction. No dedup
    here — one silver row per (entity, bronze sighting); the gold layer
    collapses across sightings. A bronze row counts as parsed once any silver
    row carries its (source, bronze_id), so a re-run skips bronze rows whose
    silver already committed. Concrete parsers define silver_model and
    implement build_entities(); bronze_model defaults to HtmlSnapshot and the
    other hooks ship with working defaults.
    """

    bronze_model: ClassVar[type[Base]] = HtmlSnapshot
    silver_model: ClassVar[type[Base]]

    def __init__(self, config: TransformConfig) -> None:
        self.config = config
        self.source = config.source
        self.html_parser = self.make_html_parser()
        # populated in run(), inside the async with
        self.db: Database

    def db_extras(self) -> dict[str, Any]:
        return {}

    def make_database(self) -> Database:
        return Database(
            database_url=self.config.database_url,
            pool_size=self.config.db_pool_size,
            **self.db_extras(),
        )

    # hook: override to provide code-level blocklist defaults per parser
    def make_html_parser(self) -> HtmlParser:
        return HtmlParser(
            {
                "blocklist_tags": self.config.blocklist_tags,
                "blocklist_attributes": self.config.blocklist_attributes,
            }
        )

    async def load_records(self) -> list[Base]:
        return await self.db.load_unparsed(self.bronze_model, self.silver_model, self.source)

    # dispatch on bronze_model — subclasses set ApiSnapshot to swap the bronze record shape.
    def decode(self, record: Base) -> Any:
        model = self.bronze_model
        if model is HtmlSnapshot:
            return self.html_parser.parse(decompress(record.html))
        if model is ApiSnapshot:
            return json.loads(decompress(record.body))
        raise ParserError(f"{self.source}: no decoder for {model}")

    @abstractmethod
    def build_entities(self, record: Base, decoded: Any) -> list[Base]: ...

    def stamp(self, entity: Base, record: Base) -> None:
        entity.bronze_id = record.bronze_id
        entity.created_at = record.created_at
        return

    def process_record(self, record: Base) -> list[Base]:
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
            self.stamp(entity, record)
        return entities

    async def run(self) -> None:
        """Lifecycle: open db; load unparsed bronze; decode + build + stamp; insert silver."""
        async with self.make_database() as db:
            self.db = db
            try:
                records = await self.load_records()
                entities: list[Base] = []
                for record in records:
                    entities.extend(self.process_record(record))
                await self.db.insert(entities, model=self.silver_model)
            except DatabaseError as exc:
                raise ParserError(f"source {self.source!r}: bronze→silver failed") from exc
        return
