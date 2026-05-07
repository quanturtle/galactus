import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass

from galactus.config import HttpConfig
from galactus.core.errors import ExtractError, ScraperError
from galactus.core.interfaces import BronzeRepo, Clock, HttpClient
from galactus.extract.base import Scraper
from galactus.extract.registry import get_scraper

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ExtractSourceSpec:
    """Per-source extract config resolved from the YAML config.

    `http` is the effective HttpConfig for this source — the domain default
    deep-merged with the source's optional override. The actual HttpClient and
    bronze db pool are opened by the stage at the start of this source's run
    and closed before the next source starts.
    """

    name: str
    scraper: str
    concurrency: int
    http: HttpConfig
    options: dict


class ExtractStage:
    """Stage 1 — internet -> bronze.

    Iterates over enabled sources, instantiates the configured scraper strategy,
    and pipes RawRecords into BronzeRepo. Per-source ScraperErrors are logged and
    skipped; anything else aborts with ExtractError. Sources run sequentially —
    cross-source parallelism is owned by Airflow. Each source opens its own
    HttpClient and BronzeRepo (via the injected factories) and closes them
    before the next source begins.
    """

    name: str = "extract"

    def __init__(
        self,
        *,
        clock: Clock,
        sources: list[ExtractSourceSpec],
        http_factory: Callable[[HttpConfig], HttpClient],
        bronze_factory: Callable[[], AbstractAsyncContextManager[BronzeRepo]],
    ) -> None:
        self.clock = clock
        self.sources = sources
        self.http_factory = http_factory
        self.bronze_factory = bronze_factory

    async def run(self) -> None:
        # iterate sources sequentially
        for spec in self.sources:

            # open per-source http + bronze; both close before the next source starts
            client = self.http_factory(spec.http)
            try:
                async with self.bronze_factory() as bronze:

                    # resolve strategy
                    cls = get_scraper(spec.scraper)
                    scraper: Scraper = cls(
                        source=spec.name,  # type: ignore[arg-type]
                        http=client,
                        clock=self.clock,
                        options=spec.options,
                        concurrency=spec.concurrency,
                    )

                    # fetch and store
                    try:
                        async for record in scraper.fetch():
                            await bronze.store(record)
                    except ScraperError as exc:
                        logger.warning("source %s failed: %s", spec.name, exc)
                        continue
                    except Exception as exc:
                        raise ExtractError(f"source {spec.name!r} aborted") from exc
            finally:
                await client.aclose()
        return
