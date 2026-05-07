import logging

from galactus.config import PipelineConfig
from galactus.core.pipeline import PipelineStage

logger = logging.getLogger(__name__)


class LoadStage(PipelineStage):
    """Stage 3 — silver -> gold.

    Stubbed for now. Aggregation logic will be ported once the silver schema is
    finalized; until then run() is a no-op. Constructor takes the same shape as
    the other stages so the wiring in cli.py stays consistent.
    """

    name: str = "load"

    def __init__(self, *, config: PipelineConfig) -> None:
        self.config = config

    async def run(self, *, sources: list[str] | None = None) -> None:
        logger.info("LoadStage.run() — no-op (stub)")
        return
