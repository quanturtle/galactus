import argparse
import asyncio
import logging
import sys

from noticias.config import settings
from noticias.db.engine import get_session
from noticias.pipeline.bronze_to_silver import run as bronze_to_silver
from noticias.scrapers import SCRAPERS


def setup_logging():
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )


async def cmd_scrape(sources: list[str]):
    for name in sources:
        cls = SCRAPERS[name]
        async with get_session() as session:
            scraper = cls(session)
            await scraper.scrape()
            await scraper.close()
        print(f"[scrape] {name}: done")


async def cmd_transform(sources: list[str] | None):
    async with get_session() as session:
        if sources:
            total = 0
            for source in sources:
                total += await bronze_to_silver(session, source=source)
        else:
            total = await bronze_to_silver(session)
        print(f"[transform] {total} articles inserted into silver")


async def cmd_run_all(sources: list[str]):
    await cmd_scrape(sources)
    await cmd_transform(sources)


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="Paraguay news scraper")
    sub = parser.add_subparsers(dest="command", required=True)

    p_scrape = sub.add_parser("scrape", help="Scrape news sites into bronze raw tables")
    p_scrape.add_argument(
        "--source",
        choices=list(SCRAPERS.keys()),
        nargs="+",
        default=list(SCRAPERS.keys()),
        help="Sources to scrape (default: all)",
    )

    p_transform = sub.add_parser("transform", help="Parse bronze raw data -> silver articles")
    p_transform.add_argument(
        "--source",
        choices=list(SCRAPERS.keys()),
        nargs="+",
        default=None,
        help="Sources to transform (default: all unparsed)",
    )

    p_all = sub.add_parser("run-all", help="Scrape + transform")
    p_all.add_argument(
        "--source",
        choices=list(SCRAPERS.keys()),
        nargs="+",
        default=list(SCRAPERS.keys()),
    )

    args = parser.parse_args()

    if args.command == "scrape":
        asyncio.run(cmd_scrape(args.source))
    elif args.command == "transform":
        asyncio.run(cmd_transform(args.source))
    elif args.command == "run-all":
        asyncio.run(cmd_run_all(args.source))


if __name__ == "__main__":
    main()
