from galactus.extract.registry import SCRAPERS
from galactus.extract.scrapers.bfs import BfsScraper


@SCRAPERS.register("abc_color")
class AbcColorScraper(BfsScraper):
    """Scraper for abc.com.py — BFS crawl."""
