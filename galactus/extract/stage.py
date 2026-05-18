import importlib
import logging

from galactus.config import PipelineConfig
from galactus.core.errors import ExtractError
from galactus.core.pipeline import PipelineStage
from galactus.extract.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class ExtractStage(PipelineStage):
    """Stage 1 — internet -> bronze.

    Resolves the configured scraper strategy and awaits scraper.run(). The
    scraper owns the HTTP client and DB pool for its run (per-site transport
    quirks live with the scraper subclass). Stage-level failure is fatal
    (wrapped as ExtractError).
    """

    name: str = "extract"

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        scraper_name = config.extract.scraper if config.extract is not None else "none"
        logger.info(
            "ExtractStage initialized (source=%s, scraper=%s)",
            config.name,
            scraper_name,
        )

    async def run(self) -> None:
        if self.config.extract is None:
            return
        ext = self.config.extract
        logger.info("extract[%s]: stage start", self.config.name)

        # resolve strategy
        mod = importlib.import_module(f"galactus.extract.scrapers.{ext.scraper}")
        scraper: BaseScraper = mod.Scraper(ext)

        # run the scraper lifecycle
        try:
            await scraper.run()
        except Exception as exc:
            raise ExtractError(f"source {self.config.name!r} aborted: {exc}") from exc
        logger.info("extract[%s]: stage complete", self.config.name)
        return
