"""Shared CLI entrypoint for domain scraper packages."""

import argparse
import asyncio
import logging
from typing import Awaitable, Callable

from galactus import db

logger = logging.getLogger(__name__)


async def run_cli(
    *,
    description: str,
    scrapers: dict[str, type],
    transform_runner: Callable[..., Awaitable[int]],
    image_downloader: Callable[..., Awaitable[int]] | None = None,
) -> None:
    parser = argparse.ArgumentParser(description=description)
    sub = parser.add_subparsers(dest="command", required=True)

    choices = list(scrapers.keys())

    p_scrape = sub.add_parser("scrape", help="Crawl sites and store raw data into bronze")
    p_scrape.add_argument("--source", choices=choices, nargs="+", default=choices,
                          help="Sources to scrape (default: all)")

    p_transform = sub.add_parser("transform", help="Parse bronze raw data -> silver")
    p_transform.add_argument("--source", choices=choices, nargs="+", default=None,
                             help="Sources to transform (default: all unparsed)")

    p_all = sub.add_parser("run-all", help="Scrape + transform")
    p_all.add_argument("--source", choices=choices, nargs="+", default=choices)

    if image_downloader is not None:
        p_images = sub.add_parser(
            "download-images",
            help="Download pending silver image rows to S3/MinIO",
        )
        p_images.add_argument("--source", choices=choices, nargs="+", default=None,
                              help="Sources to download (default: all pending)")

    args = parser.parse_args()

    await db.init()
    try:
        if args.command == "scrape":
            await _run_scrape(scrapers, args.source)
        elif args.command == "transform":
            await _run_transform(transform_runner, args.source)
        elif args.command == "run-all":
            await _run_scrape(scrapers, args.source)
            await _run_transform(transform_runner, args.source)
        elif args.command == "download-images":
            assert image_downloader is not None
            await _run_images(image_downloader, args.source)
    finally:
        await db.close()


async def _run_scrape(scrapers: dict[str, type], sources: list[str]) -> None:
    tasks = [scrapers[name]().run() for name in sources]
    await asyncio.gather(*tasks)


async def _run_transform(
    runner: Callable[..., Awaitable[int]],
    sources: list[str] | None,
) -> None:
    total = 0
    if sources:
        for source in sources:
            total += await runner(source=source)
    else:
        total = await runner()
    logger.info("%d rows inserted into silver", total)


async def _run_images(
    downloader: Callable[..., Awaitable[int]],
    sources: list[str] | None,
) -> None:
    total = 0
    if sources:
        for source in sources:
            total += await downloader(source=source)
    else:
        total = await downloader()
    logger.info("%d image rows processed", total)
