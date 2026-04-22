"""Async psycopg3 connection pool shared by all projects."""

import json
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Sequence

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

DEFAULT_DATABASE_URL = "postgresql://the_scraper:the_scraper_secret@localhost:5432/the_scraper"

_pool: AsyncConnectionPool | None = None


async def init_pool(dsn: str, *, min_size: int = 2, max_size: int = 10) -> None:
    """Create and open the global async connection pool."""
    global _pool
    if _pool is not None:
        return
    _pool = AsyncConnectionPool(dsn, min_size=min_size, max_size=max_size, open=False)
    await _pool.open()


async def init(dsn: str | None = None, **kwargs) -> None:
    """Convenience wrapper: init the pool from *dsn* or DATABASE_URL env var."""
    await init_pool(dsn or os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL), **kwargs)


async def close() -> None:
    """Convenience wrapper: close the pool."""
    await close_pool()


def get_pool() -> AsyncConnectionPool:
    if _pool is None:
        raise RuntimeError("Connection pool not initialised — call init_pool() first")
    return _pool


@asynccontextmanager
async def transaction() -> AsyncIterator[AsyncConnection]:
    """Yield a connection inside a transaction. Commits on success, rolls back on error."""
    async with get_pool().connection() as conn:
        async with conn.transaction():
            yield conn


async def execute(
    query: str,
    params=None,
    *,
    conn: AsyncConnection | None = None,
) -> list[dict]:
    """Execute a query and return rows as dicts (empty list for non-SELECT).

    When *conn* is provided, runs on that connection without committing — the
    caller's transaction owns the commit. Otherwise grabs a pooled connection
    and commits immediately.
    """
    if conn is not None:
        return await _execute_on(conn, query, params)
    async with get_pool().connection() as c:
        rows = await _execute_on(c, query, params)
        await c.commit()
        return rows


async def _execute_on(conn: AsyncConnection, query: str, params) -> list[dict]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        if cur.description:
            return await cur.fetchall()
    return []


async def bulk_insert(
    table: str,
    rows: list[dict],
    *,
    conn: AsyncConnection | None = None,
    conflict_columns: Sequence[str] | None = None,
    update_columns: Sequence[str] | None = None,
) -> None:
    """Insert rows into *table*. Column names come from the first row's keys.

    Conflict handling:
    - default: ``ON CONFLICT DO NOTHING``
    - if *update_columns* is given: ``ON CONFLICT (conflict_columns) DO UPDATE
      SET col = EXCLUDED.col …``. *conflict_columns* is required in that case.

    When *conn* is provided, runs on that connection without committing — the
    caller's transaction owns the commit. Otherwise grabs a pooled connection
    and commits immediately.
    """
    if not rows:
        return
    columns = list(rows[0].keys())
    col_names = ", ".join(columns)
    placeholders = ", ".join(f"%({c})s" for c in columns)

    if update_columns:
        if not conflict_columns:
            raise ValueError("conflict_columns is required when update_columns is set")
        conflict_target = ", ".join(conflict_columns)
        assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
        conflict_clause = f"ON CONFLICT ({conflict_target}) DO UPDATE SET {assignments}"
    else:
        conflict_clause = "ON CONFLICT DO NOTHING"

    query = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) {conflict_clause}"

    prepared = []
    for row in rows:
        r = {}
        for k, v in row.items():
            r[k] = json.dumps(v) if isinstance(v, dict) else v
        prepared.append(r)

    if conn is not None:
        await _bulk_insert_on(conn, query, prepared)
        return
    async with get_pool().connection() as c:
        await _bulk_insert_on(c, query, prepared)
        await c.commit()


async def _bulk_insert_on(conn: AsyncConnection, query: str, prepared: list[dict]) -> None:
    async with conn.cursor() as cur:
        await cur.executemany(query, prepared)


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
