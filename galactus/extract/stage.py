import importlib
import logging

from galactus.config import PipelineConfig
from galactus.core.errors import ExtractError
from galactus.core.pipeline import PipelineStage
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.db import Database
from galactus.infra.http import HttpClient

logger = logging.getLogger(__name__)


class ExtractStage(PipelineStage):
    """Stage 1 — internet -> bronze.

    Instantiates the configured scraper strategy and awaits scraper.run().
    The scraper owns its own fetch/insert lifecycle. Opens the HTTP client
    and DB pool for the run; stage-level failure is fatal (wrapped as
    ExtractError).
    """

    name: str = "extract"

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    async def run(self) -> None:
        if self.config.extract is None:
            return
        ext = self.config.extract

        # open http + db for this run
        async with (
            HttpClient(
                timeout=ext.timeout_seconds,
                headers={"User-Agent": ext.user_agent},
                retries=ext.retries,
                retry_delay=ext.retry_delay,
                concurrency=ext.concurrency,
                pool_size=ext.http_pool_size,
            ) as client,
            Database(
                database_url=self.config.database_url,
                pool_size=self.config.db_pool_size,
            ) as db,
        ):
            # resolve strategy
            mod = importlib.import_module(f"galactus.extract.scrapers.{ext.scraper}")
            scraper: BaseScraper = mod.Scraper(
                source=self.config.name,
                http=client,
                db=db,
                config=ext,
            )

            # run the scraper lifecycle
            try:
                await scraper.run()
            except Exception as exc:
                raise ExtractError(f"source {self.config.name!r} aborted") from exc
        return
