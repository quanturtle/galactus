from galactus.extract.registry import SCRAPERS
from galactus.extract.scrapers.api import ApiScraper


@SCRAPERS.register("stock")
class StockScraper(ApiScraper):
    """Scraper for stock.com.py — paginated JSON API."""
