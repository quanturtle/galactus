import sys

from galactus.discovery import discover_scrapers
from supermercados.scrapers._base import ApiScraper, BfsScraper

SCRAPERS = discover_scrapers(sys.modules[__name__], (ApiScraper, BfsScraper))
