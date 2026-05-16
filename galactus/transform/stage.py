import importlib

from galactus.config import PipelineConfig
from galactus.core.errors import TransformError
from galactus.core.pipeline import PipelineStage
from galactus.transform.base_parser import BaseParser


class TransformStage(PipelineStage):
    """Stage 2 — bronze -> silver.

    Resolves the configured parser strategy and awaits parser.run(). The
    parser owns its own DB pool for the run; stage-level failure is fatal
    (wrapped as TransformError).
    """

    name: str = "transform"

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    async def run(self) -> None:
        if self.config.transform is None:
            return
        tr = self.config.transform

        # resolve strategy
        mod = importlib.import_module(f"galactus.transform.parsers.{tr.parser}")
        parser: BaseParser = mod.Parser(tr)

        # run the parser lifecycle
        try:
            await parser.run()
        except Exception as exc:
            raise TransformError(f"source {self.config.name!r} aborted") from exc
        return
