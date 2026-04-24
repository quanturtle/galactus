"""noticias — Paraguay news scraper domain."""

from galactus.domain import make_domain
from galactus.logging import setup_logging
from galactus.scrapers.images import ImageConfig
from noticias import scrapers as _scrapers, transformers as _transformers
from noticias.article import Article
from noticias.config import settings


def _setup() -> None:
    setup_logging(settings.log_level)


IMAGES = ImageConfig(
    entity_table="silver.articles",
    image_table="silver.article_images",
    id_column="silver_article_id",
    entity_url_column="source_url",
    s3_endpoint_url=settings.s3_endpoint_url,
    s3_access_key=settings.s3_access_key,
    s3_secret_key=settings.s3_secret_key,
    s3_region=settings.s3_region,
    s3_bucket=settings.s3_bucket,
    chunk=settings.chunk_size,
    concurrency=settings.image_download_concurrency,
    timeout=settings.image_download_timeout,
)


DOMAIN = make_domain(
    name="noticias",
    description="Paraguay news scraper",
    entity_cls=Article,
    scrapers=_scrapers,
    transformers=_transformers,
    chunk_size=settings.chunk_size,
    image_config=IMAGES,
    setup=_setup,
)

__all__ = ["DOMAIN"]
