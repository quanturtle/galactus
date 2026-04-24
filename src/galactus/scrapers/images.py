"""First-class image-download scraper.

Reads bronze rows with unprocessed images and drives the generic drainers in
``galactus.images.downloader``. Constructed once by ``make_domain()`` with the
domain's transformer dispatch fns + HTML / API source lists; called via the
CLI's ``download-images`` subcommand.
"""

from dataclasses import dataclass
from typing import Callable

import httpx

from galactus.images import S3ImageStore
from galactus.images.downloader import drain_api_responses, drain_snapshots


@dataclass(frozen=True)
class ImageConfig:
    entity_table: str           # "silver.articles"
    image_table: str            # "silver.article_images"
    id_column: str              # "silver_article_id"
    entity_url_column: str      # "source_url" | "url"
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    s3_bucket: str
    chunk: int
    concurrency: int
    timeout: int


class ImageScraper:
    """Drain bronze image-pending rows to S3 + silver inventory.

    Same lifecycle shape as ApiScraper / BfsScraper: a parameterless
    constructor isn't available here because the scraper closes over
    transformer fns, so ``make_domain()`` builds and stores a singleton
    instance on the domain spec. ``run(source=...)`` returns rows processed.
    """

    def __init__(
        self,
        *,
        config: ImageConfig,
        html_transformer_fn: Callable[[str, str, str], dict | None],
        api_transformer_fn: Callable[[str, str], list[dict]],
        html_sources: list[str],
        api_sources: list[str],
    ) -> None:
        self._cfg = config
        self._html_fn = html_transformer_fn
        self._api_fn = api_transformer_fn
        self._html_sources = list(html_sources)
        self._api_sources = list(api_sources)

    async def run(self, source: str | None = None) -> int:
        cfg = self._cfg
        run_html = source is None or source in self._html_sources
        run_api = source is None or source in self._api_sources

        async with (
            httpx.AsyncClient() as http,
            S3ImageStore(
                endpoint_url=cfg.s3_endpoint_url,
                access_key=cfg.s3_access_key,
                secret_key=cfg.s3_secret_key,
                region=cfg.s3_region,
            ) as s3,
        ):
            total = 0
            if run_html and self._html_sources:
                total += await drain_snapshots(
                    entity_table=cfg.entity_table,
                    image_table=cfg.image_table,
                    id_column=cfg.id_column,
                    entity_url_column=cfg.entity_url_column,
                    transformer_fn=self._html_fn,
                    transformer_sources=self._html_sources,
                    http=http,
                    s3=s3,
                    bucket=cfg.s3_bucket,
                    source=source,
                    chunk=cfg.chunk,
                    concurrency=cfg.concurrency,
                    timeout=cfg.timeout,
                )
            if run_api and self._api_sources:
                total += await drain_api_responses(
                    entity_table=cfg.entity_table,
                    image_table=cfg.image_table,
                    id_column=cfg.id_column,
                    entity_url_column=cfg.entity_url_column,
                    transformer_fn=self._api_fn,
                    transformer_sources=self._api_sources,
                    http=http,
                    s3=s3,
                    bucket=cfg.s3_bucket,
                    source=source,
                    chunk=cfg.chunk,
                    concurrency=cfg.concurrency,
                    timeout=cfg.timeout,
                )
            return total
