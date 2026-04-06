"""the_scraper — Reusable async web scraper framework."""

from the_scraper.html_cleaner import HtmlCleaner, compress, compute_content_hash, decompress
from the_scraper.parsing import (
    IMAGE_EXCLUDE,
    build_image_urls,
    extract_body_images,
    extract_json_ld,
    extract_var_data,
    meta,
    safe_int,
)
from the_scraper.scrapers import ApiScraper, BaseScraper, BfsScraper
from the_scraper.storage import ApiStorage, SnapshotStorage

__all__ = [
    "BaseScraper",
    "ApiScraper",
    "BfsScraper",
    "ApiStorage",
    "SnapshotStorage",
    "HtmlCleaner",
    "compress",
    "decompress",
    "compute_content_hash",
    "IMAGE_EXCLUDE",
    "build_image_urls",
    "extract_body_images",
    "extract_json_ld",
    "extract_var_data",
    "meta",
    "safe_int",
]
