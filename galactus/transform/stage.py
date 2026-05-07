import logging
from dataclasses import dataclass

from galactus.core.errors import ParserError, TransformError
from galactus.core.interfaces import BronzeRepo, Clock, SilverRepo
from galactus.core.records import ParsedRecord
from galactus.core.types import SourceName
from galactus.transform.base import Parser
from galactus.transform.registry import get_parser

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
    """

    def __init__(
        self,
        *,
        bronze: BronzeRepo,
        silver: SilverRepo,
        clock: Clock,
        sources: list[TransformSourceSpec],
        batch_size: int = 100,
    ) -> None:
        self.bronze = bronze
        self.silver = silver
        self.clock = clock
        self.sources = sources
        self.batch_size = batch_size

    async def run(self) -> None:
        # iterate sources
        for spec in self.sources:
            await self._run_one(spec)
        return

    async def _run_one(self, spec: TransformSourceSpec) -> None:
        # resolve strategy
        cls = get_parser(spec.parser)
        parser: Parser = cls(
            source=SourceName(spec.name),
            clock=self.clock,
            options=spec.options,
        )

        # parse and persist
        batch: list[ParsedRecord] = []
        try:
            async for raw in self.bronze.load_unparsed(SourceName(spec.name)):
                try:
                    batch.append(parser.parse(raw))
                except ParserError as exc:
                    logger.warning("parse failed for %s %s: %s", spec.name, raw.source_url, exc)
                    continue
                if len(batch) >= self.batch_size:
                    await self._flush(batch)
                    batch = []
            if batch:
                await self._flush(batch)
        except Exception as exc:
            raise TransformError(f"source {spec.name!r} aborted") from exc
        return

    async def _flush(self, batch: list[ParsedRecord]) -> None:
        await self.silver.upsert_many(batch)
        await self.bronze.mark_parsed(r.bronze_id for r in batch)
        return
