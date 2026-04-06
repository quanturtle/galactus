"""Async psycopg3 connection pool shared by all projects."""

import json
import logging

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

_pool: AsyncConnectionPool | None = None


async def init_pool(dsn: str, *, min_size: int = 2, max_size: int = 10) -> None:
    """Create and open the global async connection pool."""
    global _pool
    if _pool is not None:
        return
    _pool = AsyncConnectionPool(dsn, min_size=min_size, max_size=max_size, open=False)
    await _pool.open()


def get_pool() -> AsyncConnectionPool:
    if _pool is None:
        raise RuntimeError("Connection pool not initialised — call init_pool() first")
    return _pool


async def execute(query: str, params=None) -> list[dict]:
    """Execute a query and return rows as dicts (empty list for non-SELECT)."""
    async with get_pool().connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            if cur.description:
                return await cur.fetchall()
        await conn.commit()
    return []


async def bulk_insert(table: str, rows: list[dict]) -> None:
    """Insert rows into *table*. Column names come from the first row's keys."""
    if not rows:
        return
    columns = list(rows[0].keys())
    col_names = ", ".join(columns)
    placeholders = ", ".join(f"%({c})s" for c in columns)
    query = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    prepared = []
    for row in rows:
        r = {}
        for k, v in row.items():
            r[k] = json.dumps(v) if isinstance(v, (dict, list)) else v
        prepared.append(r)

    async with get_pool().connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(query, prepared)
        await conn.commit()


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
