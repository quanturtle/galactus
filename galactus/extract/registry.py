from galactus.core.registry import ClassRegistry
from galactus.extract.base import Scraper

SCRAPERS = ClassRegistry[Scraper]("scraper", base=Scraper)
