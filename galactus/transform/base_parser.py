import json
from abc import ABC
from typing import Any, ClassVar

from galactus.config import TransformConfig
from galactus.core.errors import DatabaseError, ParserError
from galactus.infra.db import Database
from galactus.transform.html_parser import HtmlParser, decompress
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.base import Base


class BaseParser(ABC):
    """Template Method orchestrator for the bronze→silver lifecycle.

    run() owns the lifecycle and nothing else: open the db, load the
    bronze rows for the source whose silver row has not been written
    yet, walk each through process_record (decode → build_item →
    build_entity → stamp), and insert the silver rows for each bronze
    sighting before moving to the next. No dedup here — one silver row
    per (entity, bronze sighting); the gold layer collapses across
    sightings. A bronze row counts as parsed once any silver row
    carries its (source, bronze_id), so a re-run skips bronze rows
    whose silver already committed.

    Concrete parsers set silver_model and mix in ArticleParser or
    ProductParser to contribute build_entity + the eight extract_*
    hooks. bronze_model defaults to HtmlSnapshot. build_item defaults
    to "the decoded payload is one item"; override only for listing-
    style bronze payloads that pack many entities into one record.
    Every silver field is optional, so build_entity always produces a
    row — the parser does not filter out partial entities.
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

    # default: one bronze record carries one entity, so the decoded payload is
    # the single item. override when the bronze record packs many entities
    # (e.g. a paginated API page or a category listing) and return one item per
    # silver row the record should produce.
    def build_item(self, decoded: Any) -> list[Any]:
        return [decoded]

    # build_entity is contributed by the ArticleParser / ProductParser mixin via MRO.

    def stamp(self, entity: Base, record: Base) -> None:
        entity.bronze_id = record.bronze_id
        entity.created_at = record.created_at
        return

    def process_record(self, record: Base) -> list[Base]:
        # decode + split into items + build silver; surface subclass failures as ParserError
        try:
            decoded = self.decode(record)
            entities = [self.build_entity(item) for item in self.build_item(decoded)]
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
        """Lifecycle: open db; load unparsed bronze; decode + build + stamp; insert silver per bronze record."""
        async with self.make_database() as db:
            self.db = db
            try:
                records = await self.load_records()
                for record in records:
                    entities = self.process_record(record)
                    await self.db.insert(entities, model=self.silver_model)
            except DatabaseError as exc:
                raise ParserError(f"source {self.source!r}: bronze→silver failed") from exc
        return
