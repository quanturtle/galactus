import json
import logging
from abc import ABC
from typing import Any, ClassVar

from bs4 import BeautifulSoup

from galactus.config import TransformConfig
from galactus.core.errors import DatabaseError, ParserError
from galactus.infra.db import Database
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.base import Base

logger = logging.getLogger(__name__)


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
        # populated in run(), inside the async with
        self.db: Database
        logger.info(
            "Parser initialized (source=%s, parser=%s, bronze_model=%s, silver_model=%s)",
            self.source,
            type(self).__name__,
            self.bronze_model.__name__,
            self.silver_model.__name__,
        )

    def db_extras(self) -> dict[str, Any]:
        return {}

    def make_database(self) -> Database:
        return Database(
            database_url=self.config.database_url,
            pool_size=self.config.db_pool_size,
            **self.db_extras(),
        )

    # bronze html is already cleaned at extract time, so decode just builds the tree.
    def decode(self, record: Base) -> Any:
        if isinstance(record, HtmlSnapshot):
            return BeautifulSoup(self.db.decompress(record.body), "lxml")
        if isinstance(record, ApiSnapshot):
            return json.loads(self.db.decompress(record.body))
        raise ParserError(f"{self.source}: no decoder for {type(record).__name__}")

    # default: decoded payload is one item. override when a bronze record packs many entities.
    def build_item(self, decoded: Any) -> list[Any]:
        return [decoded]

    # build_entity is contributed by the ArticleParser / ProductParser mixin via MRO.
    def stamp(self, entity: Base, record: Base) -> None:
        entity.bronze_id = record.id
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
                f"source {self.source!r}: bronze_id {record.id} decode/build failed"
            ) from exc

        # stamp bronze provenance: which bronze row this came from, and its snapshot time
        for entity in entities:
            self.stamp(entity, record)
        return entities

    async def run(self) -> None:
        """Lifecycle: open db; stream unparsed bronze; decode + build + stamp; insert silver per record."""
        async with self.make_database() as db:
            self.db = db
            processed = 0
            skipped = 0
            silver_rows = 0
            batch_size = self.config.batch_size
            logger.info(
                "transform[%s]: parser run start (batch_size=%s)",
                self.source,
                batch_size,
            )
            try:
                async for record in self.db.stream_unparsed(
                    self.bronze_model,
                    self.silver_model,
                    self.source,
                    chunk_size=batch_size,
                ):
                    try:
                        entities = self.process_record(record)
                    except ParserError as exc:
                        skipped += 1
                        logger.warning(
                            "transform[%s]: skipping bronze_id=%s: %s: %s",
                            self.source,
                            record.id,
                            type(exc).__name__,
                            exc,
                            exc_info=True,
                        )
                        continue
                    await self.db.insert(entities, model=self.silver_model)
                    processed += 1
                    silver_rows += len(entities)
                    logger.info(
                        "transform[%s]: inserted %s %s for bronze_id=%s",
                        self.source,
                        len(entities),
                        self.silver_model.__name__,
                        record.id,
                    )
                    if processed % batch_size == 0:
                        logger.info(
                            "transform[%s]: progress checkpoint (processed=%s, skipped=%s, silver_rows=%s)",
                            self.source,
                            processed,
                            skipped,
                            silver_rows,
                        )
            except DatabaseError as exc:
                raise ParserError(f"source {self.source!r}: bronze→silver failed") from exc
        logger.info(
            "transform[%s]: parser run complete (processed=%s, skipped=%s, silver_rows=%s)",
            self.source,
            processed,
            skipped,
            silver_rows,
        )
        return
