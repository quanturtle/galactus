import argparse
import asyncio
import logging

from noticias.config import settings
from the_scraper import db
from the_scraper.logging import setup_logging
from noticias.scrapers import SCRAPERS

setup_logging(settings.log_level)
logger = logging.getLogger(__name__)


async def cmd_scrape(sources: list[str]):
    tasks = []
    for name in sources:
        cls = SCRAPERS[name]
        scraper = cls()
        tasks.append(scraper.run())

    await asyncio.gather(*tasks)


async def cmd_transform(sources: list[str] | None):
    from noticias.transforms.bronze_to_silver import run

    total = 0
    if sources:
        for source in sources:
            total += await run(source=source)
    else:
        total = await run()
    logger.info("%d rows inserted into silver", total)


async def cmd_run_all(sources: list[str]):
    await cmd_scrape(sources)
    await cmd_transform(sources)


async def main():
    parser = argparse.ArgumentParser(description="Paraguay news scraper")
    sub = parser.add_subparsers(dest="command", required=True)

    p_scrape = sub.add_parser("scrape", help="Crawl sites and store raw data into bronze")
    p_scrape.add_argument(
        "--source",
        choices=list(SCRAPERS.keys()),
        nargs="+",
        default=list(SCRAPERS.keys()),
        help="Sources to scrape (default: all)",
    )

    p_transform = sub.add_parser("transform", help="Parse bronze raw data -> silver")
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

    await db.init()
    try:
        if args.command == "scrape":
            await cmd_scrape(args.source)
        elif args.command == "transform":
            await cmd_transform(args.source)
        elif args.command == "run-all":
            await cmd_run_all(args.source)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
