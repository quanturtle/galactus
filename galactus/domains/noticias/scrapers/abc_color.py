from galactus.extract.registry import register_scraper
from galactus.extract.scrapers.bfs import BfsScraper


@register_scraper("abc_color")
class AbcColorScraper(BfsScraper):
    """Scraper for abc.com.py — BFS crawl."""
