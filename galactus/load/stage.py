import importlib
import logging

from galactus.config import PipelineConfig
from galactus.core.errors import DatabaseError, LoadError
from galactus.core.pipeline import PipelineStage

logger = logging.getLogger(__name__)


class LoadStage(PipelineStage):
    """Stage 3 — silver -> gold.

    Resolves the configured builder strategy and awaits builder.run(). Sources
    without a load block (e.g. noticias) are a no-op. The builder owns its own DB
    pool for the run; stage-level failure is fatal (wrapped as LoadError).
    """

    name: str = "load"

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        builder_name = config.load.builder if config.load is not None else "none"
        logger.info(
            "LoadStage initialized (source=%s, builder=%s)",
            config.name,
            builder_name,
        )

    async def run(self) -> None:
        if self.config.load is None:
            return
        ld = self.config.load
        logger.info("load[%s]: stage start", self.config.name)

        # resolve strategy
        mod = importlib.import_module(f"galactus.load.builders.{ld.builder}")
        builder = mod.Builder(ld)

        # run the builder lifecycle
        try:
            await builder.run()
        except DatabaseError as exc:
            raise LoadError(f"source {self.config.name!r} aborted: {exc}") from exc
        logger.info("load[%s]: stage complete", self.config.name)
        return
