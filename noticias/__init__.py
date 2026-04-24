"""noticias — Paraguay news scraper domain."""

from galactus.domain import make_domain
from galactus.logging import setup_logging
from noticias import scrapers as _scrapers, transformers as _transformers
from noticias.article import Article
from noticias.config import settings


def _setup() -> None:
    setup_logging(settings.log_level)


DOMAIN = make_domain(
    name="noticias",
    description="Paraguay news scraper",
    entity_cls=Article,
    scrapers=_scrapers,
    transformers=_transformers,
    chunk_size=settings.chunk_size,
    setup=_setup,
)

__all__ = ["DOMAIN"]
