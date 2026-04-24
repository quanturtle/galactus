"""Unified galactus CLI — wraps alembic migrations and per-domain scrapers."""

import argparse
import asyncio
import logging
from pathlib import Path

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig

from galactus import db
from galactus.domain import DomainSpec, Pipeline

logger = logging.getLogger(__name__)


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ALEMBIC_INI = REPO_ROOT / "alembic.ini"


__all__ = ["DomainSpec", "main"]


# ---- Database commands (alembic wrapper) -----------------------------------

_DB_COMMANDS = frozenset({"migrate", "downgrade", "current", "history", "stamp", "revision"})


def _alembic_config() -> AlembicConfig:
    cfg = AlembicConfig(str(ALEMBIC_INI))
    cfg.set_main_option("script_location", str(REPO_ROOT / "migrations"))
    return cfg


def register_db_commands(subparsers: argparse._SubParsersAction) -> None:
    subparsers.add_parser("migrate", help="Apply all pending migrations (alembic upgrade head)")

    p_down = subparsers.add_parser("downgrade", help="Roll back migrations")
    p_down.add_argument("-n", type=int, default=1, help="Number of steps to roll back (default: 1)")

    subparsers.add_parser("current", help="Print the current DB revision")
    subparsers.add_parser("history", help="Print the migration graph")

    p_stamp = subparsers.add_parser("stamp", help="Mark a revision as applied without running it")
    p_stamp.add_argument("revision", help="Revision hash, 'head', or 'base'")

    p_rev = subparsers.add_parser("revision", help="Create a new migration script")
    p_rev.add_argument("--autogenerate", action="store_true", help="Diff metadata against DB")
    p_rev.add_argument("-m", "--message", default=None, help="Revision message")


def _run_db(args: argparse.Namespace) -> None:
    cfg = _alembic_config()
    cmd = args.command
    if cmd == "migrate":
        alembic_command.upgrade(cfg, "head")
    elif cmd == "downgrade":
        alembic_command.downgrade(cfg, f"-{args.n}")
    elif cmd == "current":
        alembic_command.current(cfg)
    elif cmd == "history":
        alembic_command.history(cfg)
    elif cmd == "stamp":
        alembic_command.stamp(cfg, args.revision)
    elif cmd == "revision":
        alembic_command.revision(cfg, message=args.message, autogenerate=args.autogenerate)
    else:
        raise AssertionError(f"unknown db command: {cmd}")


# ---- Domain commands -------------------------------------------------------

def register_domain_commands(subparsers: argparse._SubParsersAction, spec: DomainSpec) -> None:
    choices = list(spec.pipelines.keys())

    p = subparsers.add_parser(spec.name, help=spec.description)
    sub = p.add_subparsers(dest="subcommand", required=True)

    p_scrape = sub.add_parser("scrape", help="Crawl sites and store raw data into bronze")
    p_scrape.add_argument("--source", choices=choices, nargs="+", default=choices,
                          help="Sources to scrape (default: all)")

    p_transform = sub.add_parser("transform", help="Parse bronze raw data -> silver")
    p_transform.add_argument("--source", choices=choices, nargs="+", default=None,
                             help="Sources to transform (default: all unparsed)")

    p_all = sub.add_parser("run-all", help="Scrape + transform")
    p_all.add_argument("--source", choices=choices, nargs="+", default=choices)

    if spec.image_scraper is not None:
        p_images = sub.add_parser(
            "download-images",
            help="Download pending silver image rows to S3/MinIO",
        )
        p_images.add_argument("--source", choices=choices, nargs="+", default=None,
                              help="Sources to download (default: all pending)")


async def _run_domain(spec: DomainSpec, args: argparse.Namespace) -> None:
    if spec.setup is not None:
        spec.setup()
    await db.init()
    try:
        sub = args.subcommand
        if sub == "scrape":
            await _run_scrape(spec.pipelines, args.source)
        elif sub == "transform":
            await _run_transform(spec.pipelines, args.source)
        elif sub == "run-all":
            await _run_scrape(spec.pipelines, args.source)
            await _run_transform(spec.pipelines, args.source)
        elif sub == "download-images":
            assert spec.image_scraper is not None
            await _run_images(spec.pipelines, spec.image_scraper, args.source)
        else:
            raise AssertionError(f"unknown domain subcommand: {sub}")
    finally:
        await db.close()


async def _run_scrape(pipelines: dict[str, Pipeline], sources: list[str]) -> None:
    await asyncio.gather(*(pipelines[name].scrape() for name in sources))


async def _run_transform(
    pipelines: dict[str, Pipeline],
    sources: list[str] | None,
) -> None:
    selected = sources if sources else list(pipelines.keys())
    total = 0
    for name in selected:
        total += await pipelines[name].transform()
    logger.info("%d rows inserted into silver", total)


async def _run_images(
    pipelines: dict[str, Pipeline],
    image_scraper: type,
    sources: list[str] | None,
) -> None:
    if sources:
        total = 0
        for name in sources:
            total += await pipelines[name].download_images()
    else:
        total = await image_scraper().run()
    logger.info("%d image rows processed", total)


# ---- Composition root ------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(prog="galactus")
    subparsers = parser.add_subparsers(dest="command", required=True)

    register_db_commands(subparsers)

    from noticias import DOMAIN as NOTICIAS
    from supermercados import DOMAIN as SUPERMERCADOS

    specs = {NOTICIAS.name: NOTICIAS, SUPERMERCADOS.name: SUPERMERCADOS}
    for spec in specs.values():
        register_domain_commands(subparsers, spec)

    args = parser.parse_args()
    if args.command in _DB_COMMANDS:
        _run_db(args)
    else:
        asyncio.run(_run_domain(specs[args.command], args))
