"""Project-level base scrapers that wire up supermercados-specific defaults."""

from pathlib import Path

from the_scraper.html_cleaner import HtmlCleaner
from the_scraper.scrapers.api import ApiScraper as _ApiScraper
from the_scraper.scrapers.bfs import BfsScraper as _BfsScraper
from the_scraper.storage import PsycopgApiStorage, PsycopgSnapshotStorage

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "scrapers"

ALLOWED_ATTRS = frozenset({
    "id", "class", "href", "src", "alt", "content", "property",
    "name", "type", "data-product-id", "data-product-price", "data-modo_venta",
})


class ApiScraper(_ApiScraper):
    def __init__(self):
        super().__init__(storage=PsycopgApiStorage(), config_dir=CONFIG_DIR)


class BfsScraper(_BfsScraper):
    def __init__(self):
        super().__init__(
            storage=PsycopgSnapshotStorage(),
            config_dir=CONFIG_DIR,
            html_cleaner=HtmlCleaner(allowed_attrs=ALLOWED_ATTRS),
            use_content_hash=True,
            batch_size=100,
        )
