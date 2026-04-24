"""supermercados — Paraguay supermarket scraper domain."""

from galactus.domain import make_domain
from galactus.logging import setup_logging
from supermercados import scrapers as _scrapers, transformers as _transformers
from supermercados.config import settings
from supermercados.product import Product


def _setup() -> None:
    setup_logging(settings.log_level)


DOMAIN = make_domain(
    name="supermercados",
    description="Paraguay supermarket scraper",
    entity_cls=Product,
    scrapers=_scrapers,
    transformers=_transformers,
    chunk_size=settings.chunk_size,
    setup=_setup,
)

__all__ = ["DOMAIN"]
