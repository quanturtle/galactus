"""supermercados — Paraguay supermarket scraper domain."""

from galactus.domain import make_domain
from galactus.logging import setup_logging
from galactus.scrapers.images import ImageConfig
from supermercados import scrapers as _scrapers, transformers as _transformers
from supermercados.config import settings
from supermercados.product import Product


def _setup() -> None:
    setup_logging(settings.log_level)


IMAGES = ImageConfig(
    entity_table="silver.products",
    image_table="silver.product_images",
    id_column="silver_product_id",
    entity_url_column="url",
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
    name="supermercados",
    description="Paraguay supermarket scraper",
    entity_cls=Product,
    scrapers=_scrapers,
    transformers=_transformers,
    chunk_size=settings.chunk_size,
    image_config=IMAGES,
    setup=_setup,
)

__all__ = ["DOMAIN"]
