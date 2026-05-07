import importlib
import logging

from galactus.config import PipelineConfig
from galactus.core.errors import ExtractError
from galactus.core.pipeline import PipelineStage
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.db import open_db
from galactus.infra.http import open_http

logger = logging.getLogger(__name__)


class ExtractStage(PipelineStage):
    """Stage 1 — internet -> bronze.

    Instantiates the configured scraper strategy and pipes RawRecords into
    Database.insert. Opens its own HTTP client and DB pool; both close when the
    stage completes. Failures are wrapped as ExtractError and abort the stage.
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
            open_http(timeout_seconds=ext.timeout_seconds, user_agent=ext.user_agent) as client,
            open_db(dsn=self.config.dsn) as db,
        ):
            # resolve strategy
            mod = importlib.import_module(f"galactus.extract.scrapers.{ext.scraper}")
            scraper: BaseScraper = mod.Scraper(
                source=self.config.name,
                http=client,
                options=dict(ext.options),
                concurrency=ext.concurrency,
            )

            # fetch and store
            try:
                async for record in scraper.run():
                    await db.insert(record, table=self.config.bronze_table)
            except Exception as exc:
                raise ExtractError(f"source {self.config.name!r} aborted") from exc
        return
