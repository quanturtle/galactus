import logging
from dataclasses import dataclass

from galactus.core.errors import ExtractError, ScraperError
from galactus.core.interfaces import BronzeRepo, Clock, HttpClient
from galactus.extract.base import Scraper
from galactus.extract.registry import get_scraper

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ExtractSourceSpec:
    """Per-source extract config resolved from the YAML config."""

    name: str
    scraper: str
    concurrency: int
    options: dict


class ExtractStage:
    """Stage 1 — internet -> bronze.

    Iterates over enabled sources, instantiates the configured scraper strategy,
    and pipes RawRecords into BronzeRepo. Per-source ScraperErrors are logged and
    skipped; anything else aborts with ExtractError. Sources run sequentially —
    cross-source parallelism is owned by Airflow.
    """

    name: str = "extract"

    def __init__(
        self,
        *,
        http: HttpClient,
        bronze: BronzeRepo,
        clock: Clock,
        sources: list[ExtractSourceSpec],
    ) -> None:
        self.http = http
        self.bronze = bronze
        self.clock = clock
        self.sources = sources

    async def run(self) -> None:
        # iterate sources sequentially
        for spec in self.sources:

            # resolve strategy
            cls = get_scraper(spec.scraper)
            scraper: Scraper = cls(
                source=spec.name,  # type: ignore[arg-type]
                http=self.http,
                clock=self.clock,
                options=spec.options,
                concurrency=spec.concurrency,
            )

            # fetch and store
            try:
                async for record in scraper.fetch():
                    await self.bronze.store(record)
            except ScraperError as exc:
                logger.warning("source %s failed: %s", spec.name, exc)
                continue
            except Exception as exc:
                raise ExtractError(f"source {spec.name!r} aborted") from exc
        return
