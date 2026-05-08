from galactus.extract.base_scraper import BaseScraper
from sql.a_bronze.html_snapshots import HtmlSnapshot


class Scraper(BaseScraper):
    """Scraper for megacadena — same-domain BFS into bronze.html_snapshots."""

    model = HtmlSnapshot
