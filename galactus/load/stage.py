import logging

from galactus.core.interfaces import GoldRepo, SilverRepo

logger = logging.getLogger(__name__)


class LoadStage:
    """Stage 3 — silver -> gold.

    Stubbed for now. Aggregation logic will be ported once the silver schema is
    finalized; until then run() is a no-op.
    """

    def __init__(self, *, silver: SilverRepo, gold: GoldRepo) -> None:
        self.silver = silver
        self.gold = gold

    async def run(self) -> None:
        logger.info("LoadStage.run() — no-op (stub)")
        return
