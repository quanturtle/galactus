"""Poller that downloads pending image rows and uploads them to S3/MinIO."""

import asyncio
import hashlib
import logging
import mimetypes
from datetime import datetime, timezone

import httpx

from galactus import db

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


async def download_pending(
    *,
    table: str,
    id_column: str,
    parent_table: str,
    http: httpx.AsyncClient,
    s3: S3ImageStore,
    bucket: str,
    source: str | None,
    chunk: int,
    concurrency: int,
    timeout: int,
) -> int:
    """Poll *table* for rows with ``download_status='pending'``, download and
    upload them. Returns the count of rows processed (downloaded + failed)."""

    sem = asyncio.Semaphore(concurrency)
    total = 0

    while True:
        rows = await _select_pending(table, id_column, parent_table, source, chunk)
        if not rows:
            break

        async def _guarded(row: dict) -> None:
            async with sem:
                await _handle_row(row, table, http, s3, bucket, timeout)

        await asyncio.gather(*(_guarded(r) for r in rows))
        total += len(rows)
        logger.info("%s: processed chunk of %d (running total %d)", table, len(rows), total)

    if total == 0:
        logger.info("%s: no pending rows", table)
    else:
        logger.info("%s: done — processed %d rows", table, total)
    return total


async def _select_pending(
    table: str,
    id_column: str,
    parent_table: str,
    source: str | None,
    chunk: int,
) -> list[dict]:
    query = f"""
        SELECT img.id, img.image_url, p.source
        FROM {table} img
        JOIN {parent_table} p ON p.id = img.{id_column}
        WHERE img.download_status = 'pending'
    """
    params: list = []
    if source:
        query += " AND p.source = %s"
        params.append(source)
    query += " ORDER BY img.id LIMIT %s"
    params.append(chunk)
    return await db.execute(query, params)


async def _handle_row(
    row: dict,
    table: str,
    http: httpx.AsyncClient,
    s3: S3ImageStore,
    bucket: str,
    timeout: int,
) -> None:
    image_id = row["id"]
    url = row["image_url"]

    try:
        response = await http.get(url, timeout=timeout, follow_redirects=True)
        response.raise_for_status()
        body = response.content
        content_type = response.headers.get("content-type", "application/octet-stream")
    except Exception as exc:
        logger.warning("download failed for %s id=%d url=%s: %s", table, image_id, url, exc)
        await _mark_failed(table, image_id, str(exc))
        return

    sha256 = hashlib.sha256(body).hexdigest()

    dedup = await _lookup_dedup(table, sha256)
    if dedup is not None:
        await _mark_downloaded(
            table, image_id,
            s3_bucket=dedup["s3_bucket"],
            s3_key=dedup["s3_key"],
            content_type=dedup.get("content_type") or content_type,
            file_size_bytes=dedup.get("file_size_bytes") or len(body),
            content_hash=sha256,
        )
        return

    ext = _guess_extension(content_type)
    key = _object_key(row["source"], sha256, ext)

    try:
        await s3.upload(bucket, key, body, content_type)
    except Exception as exc:
        logger.warning("upload failed for %s id=%d key=%s: %s", table, image_id, key, exc)
        await _mark_failed(table, image_id, f"upload: {exc}")
        return

    await _mark_downloaded(
        table, image_id,
        s3_bucket=bucket,
        s3_key=key,
        content_type=content_type,
        file_size_bytes=len(body),
        content_hash=sha256,
    )


async def _lookup_dedup(table: str, sha256: str) -> dict | None:
    query = f"""
        SELECT s3_bucket, s3_key, content_type, file_size_bytes
        FROM {table}
        WHERE content_hash = %s AND download_status = 'downloaded'
        LIMIT 1
    """
    rows = await db.execute(query, [sha256])
    return rows[0] if rows else None


async def _mark_failed(table: str, image_id: int, error: str) -> None:
    await db.execute(
        f"UPDATE {table} SET download_status = 'failed', download_error = %s, updated_at = NOW() WHERE id = %s",
        [error[:2000], image_id],
    )


async def _mark_downloaded(
    table: str,
    image_id: int,
    *,
    s3_bucket: str,
    s3_key: str,
    content_type: str,
    file_size_bytes: int,
    content_hash: str,
) -> None:
    await db.execute(
        f"""
        UPDATE {table}
        SET download_status = 'downloaded',
            s3_bucket = %s,
            s3_key = %s,
            content_type = %s,
            file_size_bytes = %s,
            content_hash = %s,
            download_error = NULL,
            updated_at = NOW()
        WHERE id = %s
        """,
        [s3_bucket, s3_key, content_type, file_size_bytes, content_hash, image_id],
    )
