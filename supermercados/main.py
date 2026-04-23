import asyncio

import httpx

from galactus.cli import run_cli
from galactus.images import S3ImageStore, download_pending
from galactus.logging import setup_logging
from supermercados.config import settings
from supermercados.scrapers import SCRAPERS
from supermercados.transforms.bronze_to_silver import run as transform_run

setup_logging(settings.log_level)


async def _download_images(source: str | None = None) -> int:
    async with (
        httpx.AsyncClient() as http,
        S3ImageStore(
            endpoint_url=settings.s3_endpoint_url,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
            region=settings.s3_region,
        ) as s3,
    ):
        return await download_pending(
            table="silver.product_images",
            id_column="silver_product_id",
            parent_table="silver.products",
            http=http,
            s3=s3,
            bucket=settings.s3_bucket,
            source=source,
            chunk=settings.chunk_size,
            concurrency=settings.image_download_concurrency,
            timeout=settings.image_download_timeout,
        )


if __name__ == "__main__":
    asyncio.run(run_cli(
        description="Paraguay supermarket scraper",
        scrapers=SCRAPERS,
        transform_runner=transform_run,
        image_downloader=_download_images,
    ))
