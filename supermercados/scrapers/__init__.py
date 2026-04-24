import sys

from galactus.discovery import discover_scrapers
from supermercados.scrapers._base import (
    ApiScraper,
    BfsScraper,
    ImageScraper,
    PARSER_REGISTRY,
)

SCRAPERS = discover_scrapers(sys.modules[__name__], (ApiScraper, BfsScraper))
IMAGE_SCRAPER = ImageScraper

__all__ = ["SCRAPERS", "IMAGE_SCRAPER", "PARSER_REGISTRY"]
