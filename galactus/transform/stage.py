import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass

from galactus.core.errors import ParserError, TransformError
from galactus.core.interfaces import BronzeRepo, Clock, SilverRepo
from galactus.core.records import ParsedRecord
from galactus.core.types import SourceName
from galactus.transform.base import Parser
from galactus.transform.registry import PARSERS

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TransformSourceSpec:
    """Per-source transform config resolved from the YAML config."""

    name: str
    parser: str
    options: dict


class TransformStage:
    """Stage 2 — bronze -> silver.

    For each enabled source, reads unparsed RawRecords, runs the configured parser,
    and upserts ParsedRecords into the silver repo. Marks bronze rows as parsed.
    Sources run sequentially — cross-source parallelism is owned by Airflow.
    Each source opens its own bronze + silver repos (sharing one db pool) via
    the injected factory and closes them before the next source begins.
    """

    name: str = "transform"

    def __init__(
        self,
        *,
        clock: Clock,
        sources: list[TransformSourceSpec],
        repos_factory: Callable[[], AbstractAsyncContextManager[tuple[BronzeRepo, SilverRepo]]],
        batch_size: int = 100,
    ) -> None:
        self.clock = clock
        self.sources = sources
        self.repos_factory = repos_factory
        self.batch_size = batch_size

    async def _flush(
        self, batch: list[ParsedRecord], bronze: BronzeRepo, silver: SilverRepo
    ) -> None:
        await silver.upsert_many(batch)
        await bronze.mark_parsed(r.bronze_id for r in batch)
        return

    async def run(self) -> None:
        # iterate sources sequentially
        for spec in self.sources:
            # open per-source bronze + silver (shared db pool); closed before next source
            async with self.repos_factory() as (bronze, silver):
                # resolve strategy
                cls = PARSERS.get(spec.parser)
                parser: Parser = cls(
                    source=SourceName(spec.name),
                    clock=self.clock,
                    options=spec.options,
                )

                # parse and persist
                batch: list[ParsedRecord] = []
                try:
                    async for raw in bronze.load_unparsed(SourceName(spec.name)):
                        try:
                            batch.append(parser.parse(raw))
                        except ParserError as exc:
                            logger.warning(
                                "parse failed for %s %s: %s", spec.name, raw.source_url, exc
                            )
                            continue
                        if len(batch) >= self.batch_size:
                            await self._flush(batch, bronze, silver)
                            batch = []
                    if batch:
                        await self._flush(batch, bronze, silver)
                except Exception as exc:
                    raise TransformError(f"source {spec.name!r} aborted") from exc
        return
