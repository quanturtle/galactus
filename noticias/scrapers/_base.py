"""noticias domain — scraper factory invocation."""

from pathlib import Path

from galactus.parsers import ParserPolicyRegistry
from galactus.scrapers import ImageConfig
from galactus.scrapers.factory import make_domain_scrapers
from noticias.config import settings
from noticias.parsers import DEFAULT_PARSER_KWARGS

CONFIG_DIR = Path(__file__).resolve().parent.parent / "configs"

PARSER_REGISTRY = ParserPolicyRegistry.from_configs(
    CONFIG_DIR, defaults=DEFAULT_PARSER_KWARGS,
)

IMAGES = ImageConfig(
    table="silver.article_images",
    id_column="silver_article_id",
    parent_table="silver.articles",
    s3_endpoint_url=settings.s3_endpoint_url,
    s3_access_key=settings.s3_access_key,
    s3_secret_key=settings.s3_secret_key,
    s3_region=settings.s3_region,
    s3_bucket=settings.s3_bucket,
    chunk=settings.chunk_size,
    concurrency=settings.image_download_concurrency,
    timeout=settings.image_download_timeout,
)

ApiScraper, BfsScraper, ImageScraper = make_domain_scrapers(
    config_dir=CONFIG_DIR,
    parser_registry=PARSER_REGISTRY,
    images=IMAGES,
)
