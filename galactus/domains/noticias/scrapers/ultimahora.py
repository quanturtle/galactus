from galactus.extract.registry import register_scraper
from galactus.extract.scrapers.bfs import BfsScraper


@register_scraper("ultimahora")
class UltimaHoraScraper(BfsScraper):
    """Scraper for ultimahora.com — BFS crawl of the news site.

    Concrete URL pattern and content-page predicate provided via options;
    inherits the generic BFS crawl logic from BfsScraper.
    """
