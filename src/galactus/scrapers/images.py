"""First-class image-download scraper.

Pulls pending rows from a domain's silver.{entity}_images table, downloads the
images, and uploads them to S3. Wired up by `make_domain_scrapers(images=...)`.
"""

from dataclasses import dataclass

import httpx

from galactus.images import S3ImageStore, download_pending


@dataclass(frozen=True)
class ImageConfig:
    table: str            # e.g. "silver.article_images"
    id_column: str        # e.g. "silver_article_id"
    parent_table: str     # e.g. "silver.articles"
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    s3_bucket: str
    chunk: int
    concurrency: int
    timeout: int


class ImageScraper:
    """Drain a silver.{entity}_images queue to S3.

    Runs as a sibling of ApiScraper / BfsScraper and is invoked via the CLI's
    `download-images` subcommand. Same lifecycle shape: a parameterless
    constructor (factory-closed), an async `run(source=...)` that returns
    rows processed.
    """

    def __init__(self, *, config: ImageConfig) -> None:
        self._cfg = config

    async def run(self, source: str | None = None) -> int:
        cfg = self._cfg
        async with (
            httpx.AsyncClient() as http,
            S3ImageStore(
                endpoint_url=cfg.s3_endpoint_url,
                access_key=cfg.s3_access_key,
                secret_key=cfg.s3_secret_key,
                region=cfg.s3_region,
            ) as s3,
        ):
            return await download_pending(
                table=cfg.table,
                id_column=cfg.id_column,
                parent_table=cfg.parent_table,
                http=http,
                s3=s3,
                bucket=cfg.s3_bucket,
                source=source,
                chunk=cfg.chunk,
                concurrency=cfg.concurrency,
                timeout=cfg.timeout,
            )
