from galactus.extract.base_scraper import BaseScraper
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for stock — paginated JSON API into bronze.api_snapshots."""

    bronze_model = ApiSnapshot
