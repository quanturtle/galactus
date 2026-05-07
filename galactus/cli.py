import argparse
import asyncio
import logging
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial

from galactus.config import (
    DatabaseConfig,
    HttpConfig,
    PipelineConfig,
    load_config,
    resolve_http,
)
from galactus.core.domain_registry import get_domain, import_domain
from galactus.core.errors import PipelineError
from galactus.core.interfaces import BronzeRepo, GoldRepo, HttpClient, SilverRepo
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


def make_http_client(http_config: HttpConfig) -> HttpClient:
    return HttpxClient(
        timeout=http_config.timeout_seconds,
        headers={"User-Agent": http_config.user_agent},
    )


def open_database(db_config: DatabaseConfig) -> Database:
    return Database(
        dsn=db_config.dsn,
        min_size=db_config.min_pool_size,
        max_size=db_config.max_pool_size,
    )


@asynccontextmanager
async def bronze_lifecycle(db_config: DatabaseConfig) -> AsyncIterator[BronzeRepo]:
    db = open_database(db_config)
    await db.open()
    try:
        yield PsycopgRepo(
            db,
            RepoConfig(table="bronze.html_snapshots", conflict_keys=("source", "source_url")),
        )
    finally:
        await db.close()


@asynccontextmanager
async def transform_repos_lifecycle(
    db_config: DatabaseConfig, silver_table: str
) -> AsyncIterator[tuple[BronzeRepo, SilverRepo]]:
    db = open_database(db_config)
    await db.open()
    try:
        bronze = PsycopgRepo(
            db,
            RepoConfig(table="bronze.html_snapshots", conflict_keys=("source", "source_url")),
        )
        silver = PsycopgRepo(
            db, RepoConfig(table=silver_table, conflict_keys=("source", "source_url"))
        )
        yield bronze, silver
    finally:
        await db.close()


@asynccontextmanager
async def load_repos_lifecycle(
    db_config: DatabaseConfig, silver_table: str
) -> AsyncIterator[tuple[SilverRepo, GoldRepo]]:
    db = open_database(db_config)
    await db.open()
    try:
        silver = PsycopgRepo(
            db, RepoConfig(table=silver_table, conflict_keys=("source", "source_url"))
        )
        gold = PsycopgRepo(db, RepoConfig(table="gold.unused"))
        yield silver, gold
    finally:
        await db.close()


def build_extract_stage(*, config: PipelineConfig, clock: SystemClock) -> ExtractStage:
    specs = [
        ExtractSourceSpec(
            name=source.name,
            scraper=source.extract.scraper,
            concurrency=source.extract.concurrency,
            http=resolve_http(config.http, source.http),
            options=dict(source.extract.options),
        )
        for source in config.sources
        if source.extract is not None
    ]
    return ExtractStage(
        clock=clock,
        sources=specs,
        http_factory=make_http_client,
        bronze_factory=partial(bronze_lifecycle, config.database),
    )


def build_transform_stage(
    *, config: PipelineConfig, silver_table: str, clock: SystemClock
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
    return TransformStage(
        clock=clock,
        sources=specs,
        repos_factory=partial(transform_repos_lifecycle, config.database, silver_table),
    )


def build_load_stage(*, config: PipelineConfig, silver_table: str) -> LoadStage:
    return LoadStage(repos_factory=partial(load_repos_lifecycle, config.database, silver_table))


async def run(config: PipelineConfig, stage: str | None) -> None:
    # initialize stateless infra (clock, plugin registry); db pools are per-source
    clock = SystemClock()

    # register domain plugins (must precede silver wiring — registry owns the table name)
    import_domain(config.domain)
    validate_config_against_registries(config)
    silver_table = get_domain(config.domain).silver_table

    # assemble stages — each source opens/closes its own http client and db pool
    extract = build_extract_stage(config=config, clock=clock)
    transform = build_transform_stage(config=config, silver_table=silver_table, clock=clock)
    load = build_load_stage(config=config, silver_table=silver_table)

    pipeline = Pipeline(stages=[extract, transform, load])
    await pipeline.run(stage)
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
