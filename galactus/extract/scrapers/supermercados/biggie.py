from galactus.extract.registry import SCRAPERS
from galactus.extract.scrapers.api import ApiScraper


@SCRAPERS.register("biggie")
class BiggieScraper(ApiScraper):
    """Scraper for biggie.com.py — paginated JSON API."""
