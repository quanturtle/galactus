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
    options: dict


class ExtractStage:
    """Stage 1 — internet -> bronze.

    Iterates over enabled sources, instantiates the configured scraper strategy,
    and pipes RawRecords into BronzeRepo. Per-source ScraperErrors are logged and
    skipped; anything else aborts with ExtractError.
    """

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
        # iterate sources
        for spec in self.sources:
            await self._run_one(spec)
        return

    async def _run_one(self, spec: ExtractSourceSpec) -> None:
        # resolve strategy
        cls = get_scraper(spec.scraper)
        scraper: Scraper = cls(
            source=spec.name,  # type: ignore[arg-type]
            http=self.http,
            clock=self.clock,
            options=spec.options,
        )

        # fetch and store
        try:
            async for record in scraper.fetch():
                await self.bronze.store(record)
        except ScraperError as exc:
            logger.warning("source %s failed: %s", spec.name, exc)
            return
        except Exception as exc:
            raise ExtractError(f"source {spec.name!r} aborted") from exc
        return
