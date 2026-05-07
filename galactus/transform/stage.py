import logging

from galactus.config import PipelineConfig
from galactus.core.deps import Deps
from galactus.core.errors import ParserError, TransformError
from galactus.core.interfaces import BronzeRepo, SilverRepo
from galactus.core.records import ParsedRecord
from galactus.core.types import SourceName
from galactus.infra.db import open_db
from galactus.infra.repositories import PsycopgRepo
from galactus.transform.base import Parser
from galactus.transform.registry import PARSERS

logger = logging.getLogger(__name__)


class TransformStage:
    """Stage 2 — bronze -> silver.

    For each enabled source, reads unparsed RawRecords, runs the configured parser,
    and upserts ParsedRecords into the silver repo. Marks bronze rows as parsed.
    Sources run sequentially — cross-source parallelism is owned by Airflow.
    Each source opens its own DB pool (bronze + silver share one pool) and closes
    it before the next source starts.
    """

    name: str = "transform"

    def __init__(self, *, config: PipelineConfig, deps: Deps, batch_size: int = 100) -> None:
        self.config = config
        self.deps = deps
        self.batch_size = batch_size

    async def _flush(
        self, batch: list[ParsedRecord], bronze: BronzeRepo, silver: SilverRepo
    ) -> None:
        await silver.upsert_many(batch)
        await bronze.mark_parsed(r.bronze_id for r in batch)
        return

    async def run(self, *, source: str | None = None) -> None:
        # iterate sources sequentially
        for src in self.config.sources:
            if src.transform is None:
                continue
            if source is not None and src.name != source:
                continue

            # open per-source bronze + silver (shared db pool); closed before next source
            async with open_db(self.config.database) as db:
                bronze = PsycopgRepo(
                    db, table=src.bronze_table, conflict_keys=src.bronze_conflict_keys
                )
                silver = PsycopgRepo(
                    db, table=src.silver_table, conflict_keys=src.silver_conflict_keys
                )

                # resolve strategy
                cls = PARSERS.get(src.transform.parser)
                parser: Parser = cls(
                    source=SourceName(src.name),
                    clock=self.deps.clock,
                    options=dict(src.transform.options),
                )

                # parse and persist
                batch: list[ParsedRecord] = []
                try:
                    async for raw in bronze.load_unparsed(SourceName(src.name)):
                        try:
                            batch.append(parser.parse(raw))
                        except ParserError as exc:
                            logger.warning(
                                "parse failed for %s %s: %s", src.name, raw.source_url, exc
                            )
                            continue
                        if len(batch) >= self.batch_size:
                            await self._flush(batch, bronze, silver)
                            batch = []
                    if batch:
                        await self._flush(batch, bronze, silver)
                except Exception as exc:
                    raise TransformError(f"source {src.name!r} aborted") from exc
        return
