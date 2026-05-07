import logging

from galactus.config import PipelineConfig
from galactus.core.errors import ParserError, TransformError
from galactus.core.pipeline import PipelineStage
from galactus.core.records import ParsedRecord
from galactus.core.types import SourceName
from galactus.infra.db import Database, open_db
from galactus.transform.base import Parser
from galactus.transform.registry import PARSERS

logger = logging.getLogger(__name__)


class TransformStage(PipelineStage):
    """Stage 2 — bronze -> silver.

    For each enabled source, reads unparsed RawRecords, runs the configured parser,
    and upserts ParsedRecords via Database.upsert. Marks bronze rows as parsed.
    Per-record ParserError is logged and skipped; per-source failure is fatal
    (wrapped as TransformError). Sources run sequentially — cross-source
    parallelism is owned by Airflow. Each source opens its own DB pool and
    closes it before the next source starts.
    """

    name: str = "transform"

    def __init__(self, *, config: PipelineConfig, batch_size: int = 100) -> None:
        self.config = config
        self.batch_size = batch_size

    async def _flush(
        self,
        batch: list[ParsedRecord],
        db: Database,
        *,
        bronze_table: str,
        silver_table: str,
    ) -> None:
        await db.upsert(batch, table=silver_table, conflict_keys=("source", "source_url"))
        await db.mark_parsed((r.bronze_id for r in batch), table=bronze_table)
        return

    async def run(self, *, sources: list[str] | None = None) -> None:
        # iterate sources sequentially
        for src in self.config.sources:
            if src.transform is None:
                continue
            if sources and src.name not in sources:
                continue

            # open per-source db pool; closed before next source
            async with open_db(self.config.database) as db:
                # resolve strategy
                cls = PARSERS.get(src.transform.parser)
                parser: Parser = cls(
                    source=SourceName(src.name),
                    options=dict(src.transform.options),
                )

                # parse and persist
                batch: list[ParsedRecord] = []
                try:
                    async for raw in db.load_unparsed(
                        SourceName(src.name), table=src.bronze_table
                    ):
                        try:
                            batch.append(parser.parse(raw))
                        except ParserError as exc:
                            logger.warning(
                                "parse failed for %s %s: %s", src.name, raw.source_url, exc
                            )
                            continue
                        if len(batch) >= self.batch_size:
                            await self._flush(
                                batch,
                                db,
                                bronze_table=src.bronze_table,
                                silver_table=src.silver_table,
                            )
                            batch = []
                    if batch:
                        await self._flush(
                            batch,
                            db,
                            bronze_table=src.bronze_table,
                            silver_table=src.silver_table,
                        )
                except Exception as exc:
                    raise TransformError(f"source {src.name!r} aborted") from exc
        return
