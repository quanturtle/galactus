import logging

from galactus.config import PipelineConfig, resolve_http
from galactus.core.errors import ExtractError
from galactus.core.pipeline import PipelineStage
from galactus.core.types import SourceName
from galactus.extract.base import Scraper
from galactus.extract.registry import SCRAPERS
from galactus.infra.db import open_db
from galactus.infra.http import open_http

logger = logging.getLogger(__name__)


class ExtractStage(PipelineStage):
    """Stage 1 — internet -> bronze.

    Iterates over enabled sources, instantiates the configured scraper strategy,
    and pipes RawRecords into Database.insert. Per-source failures are fatal:
    they are wrapped as ExtractError and abort the whole stage. Sources run
    sequentially — cross-source parallelism is owned by Airflow. Each source
    opens its own HTTP client and DB pool and closes them before the next source
    begins.
    """

    name: str = "extract"

    def __init__(self, *, config: PipelineConfig) -> None:
        self.config = config

    async def run(self, *, sources: list[str] | None = None) -> None:
        # iterate sources sequentially
        for src in self.config.sources:
            if src.extract is None:
                continue
            if sources and src.name not in sources:
                continue

            # open per-source http + db; both close before the next source starts
            http_cfg = resolve_http(self.config.http, src.http)
            async with open_http(http_cfg) as client, open_db(self.config.database) as db:
                # resolve strategy
                cls = SCRAPERS.get(src.extract.scraper)
                scraper: Scraper = cls(
                    source=SourceName(src.name),
                    http=client,
                    options=dict(src.extract.options),
                    concurrency=src.extract.concurrency,
                )

                # fetch and store
                try:
                    async for record in scraper.fetch():
                        await db.insert(record, table=src.bronze_table)
                except Exception as exc:
                    raise ExtractError(f"source {src.name!r} aborted") from exc
        return
