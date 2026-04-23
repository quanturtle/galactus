import asyncio

from galactus.cli import run_cli
from galactus.logging import setup_logging
from noticias.config import settings
from noticias.scrapers import SCRAPERS
from noticias.transforms.bronze_to_silver import run as transform_run

setup_logging(settings.log_level)


if __name__ == "__main__":
    asyncio.run(run_cli(
        description="Paraguay news scraper",
        scrapers=SCRAPERS,
        transform_runner=transform_run,
    ))
