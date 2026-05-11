import importlib
import logging

from galactus.config import PipelineConfig
from galactus.core.errors import TransformError
from galactus.core.pipeline import PipelineStage
from galactus.infra.db import Database
from galactus.transform.base_parser import BaseParser

logger = logging.getLogger(__name__)


class TransformStage(PipelineStage):
    """Stage 2 — bronze -> silver.

    Instantiates the configured parser strategy and awaits parser.run().
    The parser owns its own load/parse/upsert/mark-parsed lifecycle. Opens
    the DB pool for the run; stage-level failure is fatal (wrapped as
    TransformError).
    """

    name: str = "transform"

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    async def run(self) -> None:
        if self.config.transform is None:
            return
        tr = self.config.transform

        # open per-run db pool
        async with Database(
            database_url=self.config.database_url,
            pool_size=self.config.db_pool_size,
        ) as db:
            # resolve strategy
            mod = importlib.import_module(f"galactus.transform.parsers.{tr.parser}")
            parser: BaseParser = mod.Parser(
                source=self.config.name,
                db=db,
                options=tr.options,
            )

            # run the parser lifecycle
            try:
                await parser.run()
            except Exception as exc:
                raise TransformError(f"source {self.config.name!r} aborted") from exc
        return
