from galactus.extract.registry import SCRAPERS
from galactus.extract.scrapers.bfs import BfsScraper


@SCRAPERS.register("ultimahora")
class UltimaHoraScraper(BfsScraper):
    """Scraper for ultimahora.com — BFS crawl of the news site.

    Concrete URL pattern and content-page predicate provided via options;
    inherits the generic BFS crawl logic from BfsScraper.
    """
