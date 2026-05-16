import importlib
import logging

from galactus.config import PipelineConfig
from galactus.core.errors import TransformError
from galactus.core.pipeline import PipelineStage
from galactus.transform.base_parser import BaseParser

logger = logging.getLogger(__name__)


class TransformStage(PipelineStage):
    """Stage 2 — bronze -> silver.

    Resolves the configured parser strategy and awaits parser.run(). The
    parser owns its own DB pool for the run; stage-level failure is fatal
    (wrapped as TransformError).
    """

    name: str = "transform"

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        parser_name = config.transform.parser if config.transform is not None else "none"
        logger.info(
            "TransformStage initialized (source=%s, parser=%s)",
            config.name, parser_name,
        )

    async def run(self) -> None:
        if self.config.transform is None:
            return
        tr = self.config.transform
        logger.info("transform[%s]: stage start", self.config.name)

        # resolve strategy
        mod = importlib.import_module(f"galactus.transform.parsers.{tr.parser}")
        parser: BaseParser = mod.Parser(tr)

        # run the parser lifecycle
        try:
            await parser.run()
        except Exception as exc:
            raise TransformError(f"source {self.config.name!r} aborted: {exc}") from exc
        logger.info("transform[%s]: stage complete", self.config.name)
        return
