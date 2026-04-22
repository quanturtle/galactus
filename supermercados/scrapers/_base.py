"""Project-level base scrapers that wire up supermercados-specific defaults."""

from pathlib import Path

from galactus.html_cleaner import HtmlCleaner
from galactus.scrapers.api import ApiScraper as _ApiScraper
from galactus.scrapers.bfs import BfsScraper as _BfsScraper
from galactus.storage import PsycopgApiStorage, PsycopgSnapshotStorage

CONFIG_DIR = Path(__file__).resolve().parent.parent / "configs"

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
