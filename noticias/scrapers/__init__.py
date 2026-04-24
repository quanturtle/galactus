import sys

from galactus.discovery import discover_scrapers
from noticias.scrapers._base import (
    ApiScraper,
    BfsScraper,
    PARSER_REGISTRY,
)

SCRAPERS = discover_scrapers(sys.modules[__name__], (ApiScraper, BfsScraper))

__all__ = ["SCRAPERS", "PARSER_REGISTRY"]
