import argparse
import asyncio
import logging
import sys

from supermercados import db
from supermercados.scrapers import ALL_SCRAPERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def scrape(stores: list[str]):
    tasks = []
    for name in stores:
        cls = ALL_SCRAPERS[name]
        scraper = cls()
        tasks.append(scraper.run())

    await asyncio.gather(*tasks)


def main():
    parser = argparse.ArgumentParser(description="Supermarket HTML snapshot crawler - Paraguay")
    sub = parser.add_subparsers(dest="command")

    sp = sub.add_parser("scrape", help="Crawl sites and store HTML snapshots")
    sp.add_argument(
        "--store",
        choices=[*ALL_SCRAPERS.keys(), "all"],
        default="all",
        help="Which store to scrape (default: all)",
    )

    tp = sub.add_parser("transform", help="Parse bronze raw data → silver products")
    tp.add_argument(
        "--source",
        choices=[*ALL_SCRAPERS.keys(), "all"],
        default="all",
    )

    args = parser.parse_args()

    try:
        if args.command == "scrape":
            stores = list(ALL_SCRAPERS.keys()) if args.store == "all" else [args.store]
            asyncio.run(scrape(stores))
        elif args.command == "transform":
            from supermercados.transforms.bronze_to_silver import run
            source = None if args.source == "all" else args.source
            run(source)
        else:
            parser.print_help()
            sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
