from galactus.extract.registry import register_scraper
from galactus.extract.scrapers.api import ApiScraper


@register_scraper("stock")
class StockScraper(ApiScraper):
    """Scraper for stock.com.py — paginated JSON API."""
