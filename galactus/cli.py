import argparse
import asyncio
import logging
import sys

from galactus.config import PipelineConfig, load_config
from galactus.core.domain_registry import get_domain, import_domain
from galactus.core.errors import PipelineError
from galactus.core.pipeline import Pipeline
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
        default=None,
        help="run only one stage by name (default: run all stages in order)",
    )
    return parser.parse_args(argv)


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
            concurrency=source.extract.concurrency,
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


async def run(config: PipelineConfig, stage: str | None) -> None:
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

    # register domain plugins (must precede silver wiring — registry owns the table name)
    import_domain(config.domain)
    validate_config_against_registries(config)

    # build repositories — same class, configured per layer
    bronze_html = PsycopgRepo(
        db, RepoConfig(table="bronze.html_snapshots", conflict_keys=("source", "source_url"))
    )
    silver_table = get_domain(config.domain).silver_table
    silver = PsycopgRepo(db, RepoConfig(table=silver_table, conflict_keys=("source", "source_url")))
    gold = PsycopgRepo(db, RepoConfig(table="gold.unused"))

    # assemble stages
    extract = build_extract_stage(config=config, http=http, bronze=bronze_html, clock=clock)
    transform = build_transform_stage(config=config, bronze=bronze_html, silver=silver, clock=clock)
    load = LoadStage(silver=silver, gold=gold)

    pipeline = Pipeline(stages=[extract, transform, load])

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
