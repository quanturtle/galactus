"""Project-level base scrapers that wire up supermercados-specific defaults."""

from pathlib import Path

from galactus.scrapers.factory import make_domain_scrapers

CONFIG_DIR = Path(__file__).resolve().parent.parent / "configs"

ALLOWED_ATTRS = frozenset({
    "id", "class", "href", "src", "alt", "content", "property",
    "name", "type", "data-product-id", "data-product-price", "data-modo_venta",
})

ApiScraper, BfsScraper = make_domain_scrapers(
    config_dir=CONFIG_DIR,
    allowed_attrs=ALLOWED_ATTRS,
    batch_size=100,
)
