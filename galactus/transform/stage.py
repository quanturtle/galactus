import importlib
import logging

from galactus.config import PipelineConfig
from galactus.core.errors import ParserError, TransformError
from galactus.core.pipeline import PipelineStage
from galactus.core.records import ParsedRecord
from galactus.infra.db import Database, open_db
from galactus.transform.base import Parser

logger = logging.getLogger(__name__)


class TransformStage(PipelineStage):
    """Stage 2 — bronze -> silver.

    Reads unparsed RawRecords, runs the configured parser, and upserts
    ParsedRecords via Database.upsert. Marks bronze rows as parsed. Per-record
    ParserError is logged and skipped; stage-level failure is fatal (wrapped as
    TransformError). Opens its own DB pool and closes it when the stage ends.
    """

    name: str = "transform"

    def __init__(self, config: PipelineConfig, batch_size: int = 100) -> None:
        self.config = config
        self.batch_size = batch_size

    async def _flush(
        self,
        batch: list[ParsedRecord],
        db: Database,
        bronze_table: str,
        silver_table: str,
    ) -> None:
        await db.upsert(batch, table=silver_table)
        await db.mark_parsed([r.bronze_id for r in batch], table=bronze_table)
        return

    async def run(self) -> None:
        if self.config.transform is None:
            return

        # open per-run db pool
        async with open_db(dsn=self.config.dsn) as db:
            # resolve strategy
            mod = importlib.import_module(f"galactus.transform.parsers.{self.config.transform.parser}")
            parser: Parser = mod.Parser(
                source=self.config.name,
                options=dict(self.config.transform.options),
            )

            # parse and persist
            batch: list[ParsedRecord] = []
            try:
                async for raw in db.load_unparsed(
                    self.config.name, table=self.config.bronze_table
                ):
                    try:
                        batch.append(parser.run(raw))
                    except ParserError as exc:
                        logger.warning(
                            "parse failed for %s %s: %s",
                            self.config.name,
                            raw.source_url,
                            exc,
                        )
                        continue
                    if len(batch) >= self.batch_size:
                        await self._flush(
                            batch,
                            db,
                            bronze_table=self.config.bronze_table,
                            silver_table=self.config.silver_table,
                        )
                        batch = []
                if batch:
                    await self._flush(
                        batch,
                        db,
                        bronze_table=self.config.bronze_table,
                        silver_table=self.config.silver_table,
                    )
            except Exception as exc:
                raise TransformError(f"source {self.config.name!r} aborted") from exc
        return
