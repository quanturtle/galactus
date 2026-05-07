from galactus.extract.registry import register_scraper
from galactus.extract.scrapers.api import ApiScraper


@register_scraper("biggie")
class BiggieScraper(ApiScraper):
    """Scraper for biggie.com.py — paginated JSON API."""
