import argparse
import asyncio
import importlib
import logging
import sys
from typing import get_args

from galactus.config import PipelineConfig, load_config
from galactus.core.errors import PipelineError
from galactus.core.types import Stage
from galactus.extract.registry import get_scraper
from galactus.extract.stage import ExtractSourceSpec, ExtractStage
from galactus.infra.clock import SystemClock
from galactus.infra.db import Database
from galactus.infra.http import HttpxClient
from galactus.infra.logging import setup_logging
from galactus.infra.repositories import PsycopgRepo, RepoConfig
from galactus.load.stage import LoadStage
from galactus.transform.registry import get_parser
from galactus.transform.stage import TransformSourceSpec, TransformStage

logger = logging.getLogger(__name__)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="galactus")
    parser.add_argument("config", help="path to YAML config file")
    parser.add_argument(
        "--stage",
        choices=list(get_args(Stage)),
        default=None,
        help="run only one stage (default: extract then transform; load is stubbed)",
    )
    return parser.parse_args(argv)


def import_domain(domain_name: str) -> None:
    """Importing a domain triggers the @register_scraper / @register_parser decorators.

    After this call, the registries contain every scraper/parser the domain ships.
    """
    importlib.import_module(f"galactus.domains.{domain_name}")
    return


def validate_config_against_registries(config: PipelineConfig) -> None:
    """Fail fast if the YAML references unknown scraper or parser names."""
    for source in config.sources:
        if source.extract is not None:
            get_scraper(source.extract.scraper)
        if source.transform is not None:
            get_parser(source.transform.parser)
    return


def build_extract_stage(
    *, config: PipelineConfig, http: HttpxClient, bronze: PsycopgRepo, clock: SystemClock
) -> ExtractStage:
    specs = [
        ExtractSourceSpec(
            name=source.name,
            scraper=source.extract.scraper,
            options=dict(source.extract.options),
        )
        for source in config.sources
        if source.extract is not None
    ]
    return ExtractStage(http=http, bronze=bronze, clock=clock, sources=specs)


def build_transform_stage(
    *, config: PipelineConfig, bronze: PsycopgRepo, silver: PsycopgRepo, clock: SystemClock
) -> TransformStage:
    specs = [
        TransformSourceSpec(
            name=source.name,
            parser=source.transform.parser,
            options=dict(source.transform.options),
        )
        for source in config.sources
        if source.transform is not None
    ]
    return TransformStage(bronze=bronze, silver=silver, clock=clock, sources=specs)


def resolve_silver_table(schema_module: str) -> str:
    """Look up SILVER_TABLE from the domain's schema module."""
    module = importlib.import_module(schema_module)
    return module.SILVER_TABLE


class Pipeline:
    """Composition root — owns the constructed stages and runs them in order.

    Pipeline is the only place that knows about both stage interfaces and
    infra implementations. Stages are constructed once and reused for the run.
    """

    def __init__(
        self,
        *,
        extract: ExtractStage,
        transform: TransformStage,
        load: LoadStage,
    ) -> None:
        self.extract = extract
        self.transform = transform
        self.load = load

    async def run(self, stage: Stage | None = None) -> None:
        if stage is None:
            await self.extract.run()
            await self.transform.run()
            await self.load.run()
            return
        if stage == "extract":
            await self.extract.run()
            return
        if stage == "transform":
            await self.transform.run()
            return
        if stage == "load":
            await self.load.run()
            return
        return


async def run(config: PipelineConfig, stage: Stage | None) -> None:
    # initialize infra
    clock = SystemClock()
    http = HttpxClient(
        timeout=config.http.timeout_seconds,
        headers={"User-Agent": config.http.user_agent},
    )
    db = Database(
        dsn=config.database.dsn,
        min_size=config.database.min_pool_size,
        max_size=config.database.max_pool_size,
    )
    await db.open()

    # build repositories — same class, configured per layer
    bronze_html = PsycopgRepo(
        db, RepoConfig(table="bronze.html_snapshots", conflict_keys=("source", "source_url"))
    )
    silver_table = resolve_silver_table(config.schema_module)
    silver = PsycopgRepo(db, RepoConfig(table=silver_table, conflict_keys=("source", "source_url")))
    gold = PsycopgRepo(db, RepoConfig(table="gold.unused"))

    # register domain plugins
    import_domain(config.domain)
    validate_config_against_registries(config)

    # assemble stages
    extract = build_extract_stage(config=config, http=http, bronze=bronze_html, clock=clock)
    transform = build_transform_stage(config=config, bronze=bronze_html, silver=silver, clock=clock)
    load = LoadStage(silver=silver, gold=gold)

    pipeline = Pipeline(extract=extract, transform=transform, load=load)

    # execute, then cleanly tear down
    try:
        await pipeline.run(stage)
    finally:
        await http.aclose()
        await db.close()
    return


def main() -> int:
    args = parse_args(sys.argv[1:])
    config = load_config(args.config)
    setup_logging(config.log_level)
    try:
        asyncio.run(run(config, args.stage))
    except PipelineError as exc:
        logger.error("pipeline failed: %s", exc)
        return 1
    return 0


main()
