"""Bronze-driven image downloader.

Reads bronze rows with unprocessed images (``images_processed_at IS NULL``,
guarded by ``parsed_at IS NOT NULL`` so silver is populated), re-runs the
domain's transformer to extract image URLs, downloads each URL, uploads to
S3, and inserts a ``silver.*_images`` row only on upload success. Marks the
bronze row processed on every outcome — no retry on per-URL failure.
"""

import asyncio
import hashlib
import logging
import mimetypes
from datetime import datetime, timezone
from typing import Callable

import httpx

from galactus import db
from galactus.html_cleaner import decompress

from .storage import S3ImageStore

logger = logging.getLogger(__name__)


_EXT_OVERRIDES = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/svg+xml": ".svg",
    "image/avif": ".avif",
}


def _guess_extension(content_type: str) -> str:
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct in _EXT_OVERRIDES:
        return _EXT_OVERRIDES[ct]
    return mimetypes.guess_extension(ct) or ".bin"


def _object_key(source: str, sha256: str, ext: str) -> str:
    now = datetime.now(tz=timezone.utc)
    return f"{source}/{now.year:04d}/{now.month:02d}/{sha256}{ext}"


async def drain_snapshots(
    *,
    entity_table: str,
    image_table: str,
    id_column: str,
    entity_url_column: str,
    transformer_fn: Callable[[str, str, str], dict | None],
    transformer_sources: list[str],
    http: httpx.AsyncClient,
    s3: S3ImageStore,
    bucket: str,
    source: str | None,
    chunk: int,
    concurrency: int,
    timeout: int,
) -> int:
    sem = asyncio.Semaphore(concurrency)
    total = 0
    while True:
        rows = await _select_snapshots(transformer_sources, source, chunk)
        if not rows:
            break
        for row in rows:
            await _process_snapshot(
                row,
                entity_table=entity_table,
                image_table=image_table,
                id_column=id_column,
                entity_url_column=entity_url_column,
                transformer_fn=transformer_fn,
                http=http, s3=s3, bucket=bucket,
                timeout=timeout, sem=sem,
            )
        total += len(rows)
        logger.info(
            "snapshots images: processed chunk of %d (running total %d)",
            len(rows), total,
        )
    if total == 0:
        logger.info("snapshots images: no bronze rows with pending images")
    else:
        logger.info("snapshots images: done — processed %d rows", total)
    return total


async def drain_api_responses(
    *,
    entity_table: str,
    image_table: str,
    id_column: str,
    entity_url_column: str,
    transformer_fn: Callable[[str, str], list[dict]],
    transformer_sources: list[str],
    http: httpx.AsyncClient,
    s3: S3ImageStore,
    bucket: str,
    source: str | None,
    chunk: int,
    concurrency: int,
    timeout: int,
) -> int:
    sem = asyncio.Semaphore(concurrency)
    total = 0
    while True:
        rows = await _select_api_responses(transformer_sources, source, chunk)
        if not rows:
            break
        for row in rows:
            await _process_api_response(
                row,
                entity_table=entity_table,
                image_table=image_table,
                id_column=id_column,
                entity_url_column=entity_url_column,
                transformer_fn=transformer_fn,
                http=http, s3=s3, bucket=bucket,
                timeout=timeout, sem=sem,
            )
        total += len(rows)
        logger.info(
            "api_responses images: processed chunk of %d (running total %d)",
            len(rows), total,
        )
    if total == 0:
        logger.info("api_responses images: no bronze rows with pending images")
    else:
        logger.info("api_responses images: done — processed %d rows", total)
    return total


async def _select_snapshots(
    transformer_sources: list[str], source: str | None, chunk: int,
) -> list[dict]:
    query = """
        SELECT id, source, url, html_blob
        FROM bronze.snapshots
        WHERE images_processed_at IS NULL
          AND parsed_at IS NOT NULL
          AND source = ANY(%(sources)s)
    """
    params: dict = {"sources": transformer_sources, "chunk": chunk}
    if source:
        query += " AND source = %(source)s"
        params["source"] = source
    query += " ORDER BY id LIMIT %(chunk)s"
    return await db.execute(query, params)


async def _select_api_responses(
    transformer_sources: list[str], source: str | None, chunk: int,
) -> list[dict]:
    query = """
        SELECT id, source, endpoint, response_blob
        FROM bronze.api_responses
        WHERE images_processed_at IS NULL
          AND parsed_at IS NOT NULL
          AND source = ANY(%(sources)s)
    """
    params: dict = {"sources": transformer_sources, "chunk": chunk}
    if source:
        query += " AND source = %(source)s"
        params["source"] = source
    query += " ORDER BY id LIMIT %(chunk)s"
    return await db.execute(query, params)


async def _process_snapshot(
    row: dict,
    *,
    entity_table: str,
    image_table: str,
    id_column: str,
    entity_url_column: str,
    transformer_fn: Callable,
    http: httpx.AsyncClient,
    s3: S3ImageStore,
    bucket: str,
    timeout: int,
    sem: asyncio.Semaphore,
) -> None:
    try:
        html = decompress(bytes(row["html_blob"]))
        parsed = transformer_fn(row["source"], html, row["url"])
    except Exception:
        logger.exception(
            "Failed to transform snapshot %d (%s)", row["id"], row["url"],
        )
        parsed = None

    images = (parsed or {}).get("images") or []
    if images:
        silver_id = await _resolve_silver_id(
            entity_table, entity_url_column, row["source"], row["url"],
        )
        if silver_id is not None:
            await _handle_image_urls(
                images,
                source=row["source"],
                silver_id=silver_id,
                image_table=image_table,
                id_column=id_column,
                http=http, s3=s3, bucket=bucket,
                timeout=timeout, sem=sem,
            )

    await db.execute(
        "UPDATE bronze.snapshots SET images_processed_at = NOW() WHERE id = %s",
        [row["id"]],
    )


async def _process_api_response(
    row: dict,
    *,
    entity_table: str,
    image_table: str,
    id_column: str,
    entity_url_column: str,
    transformer_fn: Callable,
    http: httpx.AsyncClient,
    s3: S3ImageStore,
    bucket: str,
    timeout: int,
    sem: asyncio.Semaphore,
) -> None:
    try:
        response_text = decompress(bytes(row["response_blob"]))
        entries = transformer_fn(row["source"], response_text) or []
    except Exception:
        logger.exception(
            "Failed to transform api_response %d (%s)", row["id"], row["endpoint"],
        )
        entries = []

    for entry in entries:
        images = entry.get("images") or []
        entity_url = entry.get(entity_url_column)
        if not images or not entity_url:
            continue
        silver_id = await _resolve_silver_id(
            entity_table, entity_url_column, row["source"], entity_url,
        )
        if silver_id is None:
            continue
        await _handle_image_urls(
            images,
            source=row["source"],
            silver_id=silver_id,
            image_table=image_table,
            id_column=id_column,
            http=http, s3=s3, bucket=bucket,
            timeout=timeout, sem=sem,
        )

    await db.execute(
        "UPDATE bronze.api_responses SET images_processed_at = NOW() WHERE id = %s",
        [row["id"]],
    )


async def _resolve_silver_id(
    entity_table: str, entity_url_column: str, source: str, url: str,
) -> int | None:
    rows = await db.execute(
        f"SELECT id FROM {entity_table} "
        f"WHERE source = %s AND {entity_url_column} = %s LIMIT 1",
        [source, url],
    )
    return rows[0]["id"] if rows else None


async def _handle_image_urls(
    urls: list[str],
    *,
    source: str,
    silver_id: int,
    image_table: str,
    id_column: str,
    http: httpx.AsyncClient,
    s3: S3ImageStore,
    bucket: str,
    timeout: int,
    sem: asyncio.Semaphore,
) -> None:
    async def _one(ordinal: int, url: str) -> None:
        async with sem:
            await _download_and_store(
                url=url, ordinal=ordinal,
                source=source, silver_id=silver_id,
                image_table=image_table, id_column=id_column,
                http=http, s3=s3, bucket=bucket, timeout=timeout,
            )
    await asyncio.gather(*(_one(i, url) for i, url in enumerate(urls)))


async def _download_and_store(
    *,
    url: str,
    ordinal: int,
    source: str,
    silver_id: int,
    image_table: str,
    id_column: str,
    http: httpx.AsyncClient,
    s3: S3ImageStore,
    bucket: str,
    timeout: int,
) -> None:
    try:
        response = await http.get(url, timeout=timeout, follow_redirects=True)
        response.raise_for_status()
        body = response.content
        content_type = response.headers.get("content-type", "application/octet-stream")
    except Exception as exc:
        logger.warning("download failed for %s url=%s: %s", image_table, url, exc)
        return

    sha256 = hashlib.sha256(body).hexdigest()

    dedup = await _lookup_dedup(image_table, sha256)
    if dedup is not None:
        s3_bucket = dedup["s3_bucket"]
        s3_key = dedup["s3_key"]
        stored_type = dedup.get("content_type") or content_type
        file_size = dedup.get("file_size_bytes") or len(body)
    else:
        ext = _guess_extension(content_type)
        s3_key = _object_key(source, sha256, ext)
        try:
            await s3.upload(bucket, s3_key, body, content_type)
        except Exception as exc:
            logger.warning("upload failed for %s key=%s: %s", image_table, s3_key, exc)
            return
        s3_bucket = bucket
        stored_type = content_type
        file_size = len(body)

    await _insert_image_row(
        image_table=image_table,
        id_column=id_column,
        silver_id=silver_id,
        url=url,
        ordinal=ordinal,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        content_type=stored_type,
        file_size_bytes=file_size,
        content_hash=sha256,
    )


async def _lookup_dedup(image_table: str, sha256: str) -> dict | None:
    rows = await db.execute(
        f"SELECT s3_bucket, s3_key, content_type, file_size_bytes "
        f"FROM {image_table} WHERE content_hash = %s LIMIT 1",
        [sha256],
    )
    return rows[0] if rows else None


async def _insert_image_row(
    *,
    image_table: str,
    id_column: str,
    silver_id: int,
    url: str,
    ordinal: int,
    s3_bucket: str,
    s3_key: str,
    content_type: str,
    file_size_bytes: int,
    content_hash: str,
) -> None:
    image_role = "hero" if ordinal == 0 else "body"
    await db.execute(
        f"""
        INSERT INTO {image_table}
            ({id_column}, image_url, image_role, ordinal,
             s3_bucket, s3_key, content_type, file_size_bytes, content_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ({id_column}, image_url) DO NOTHING
        """,
        [silver_id, url, image_role, ordinal,
         s3_bucket, s3_key, content_type, file_size_bytes, content_hash],
    )
