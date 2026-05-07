import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager

from galactus.core.interfaces import GoldRepo, SilverRepo

logger = logging.getLogger(__name__)


class LoadStage:
    """Stage 3 — silver -> gold.

    Stubbed for now. Aggregation logic will be ported once the silver schema is
    finalized; until then run() is a no-op. Constructor takes the same kind of
    factory the other stages use so the wiring in cli.py stays consistent.
    """

    name: str = "load"

    def __init__(
        self,
        *,
        repos_factory: Callable[[], AbstractAsyncContextManager[tuple[SilverRepo, GoldRepo]]],
    ) -> None:
        self.repos_factory = repos_factory

    async def run(self) -> None:
        logger.info("LoadStage.run() — no-op (stub)")
        return
