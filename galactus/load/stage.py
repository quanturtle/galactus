import logging

from galactus.config import PipelineConfig
from galactus.core.deps import Deps

logger = logging.getLogger(__name__)


class LoadStage:
    """Stage 3 — silver -> gold.

    Stubbed for now. Aggregation logic will be ported once the silver schema is
    finalized; until then run() is a no-op. Constructor takes the same shape as
    the other stages so the wiring in cli.py stays consistent.
    """

    name: str = "load"

    def __init__(self, *, config: PipelineConfig, deps: Deps) -> None:
        self.config = config
        self.deps = deps

    async def run(self, *, source: str | None = None) -> None:
        logger.info("LoadStage.run() — no-op (stub)")
        return
