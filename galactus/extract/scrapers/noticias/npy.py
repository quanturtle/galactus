from galactus.extract.base_scraper import BaseScraper
from sql.a_bronze.html_snapshots import HtmlSnapshot


class Scraper(BaseScraper):
    """Scraper for npy — same-domain BFS into bronze.html_snapshots."""

    bronze_model = HtmlSnapshot
