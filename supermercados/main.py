import asyncio

from galactus.cli import run_cli
from galactus.logging import setup_logging
from supermercados.config import settings
from supermercados.scrapers import SCRAPERS
from supermercados.transforms.bronze_to_silver import run as transform_run

setup_logging(settings.log_level)


if __name__ == "__main__":
    asyncio.run(run_cli(
        description="Paraguay supermarket scraper",
        scrapers=SCRAPERS,
        transform_runner=transform_run,
    ))
