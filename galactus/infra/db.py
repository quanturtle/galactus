from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from galactus.config import DatabaseConfig


class Database:
    """Async psycopg connection pool wrapper.

    Holds a single AsyncConnectionPool and exposes thin helpers for query and
    bulk insert. Repositories receive a Database instance via the constructor
    rather than reading DSN env vars directly.
    """

    def __init__(self, *, dsn: str, min_size: int = 1, max_size: int = 10) -> None:
        self._dsn = dsn
        self._pool = AsyncConnectionPool(
            conninfo=dsn,
            min_size=min_size,
            max_size=max_size,
            open=False,
        )

    async def open(self) -> None:
        await self._pool.open()
        return

    async def close(self) -> None:
        await self._pool.close()
        return

    async def fetch_all(self, sql: str, params: Sequence[Any] | None = None) -> list[dict]:
        async with (
            self._pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cur,
        ):
            await cur.execute(sql, params)
            return await cur.fetchall()

    async def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
        return

    async def execute_many(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.executemany(sql, list(rows))
        return


@asynccontextmanager
async def open_db(config: DatabaseConfig) -> AsyncIterator[Database]:
    """Open a Database pool from config and close it on exit. Used per-source by stages."""
    db = Database(
        dsn=config.dsn,
        min_size=config.min_pool_size,
        max_size=config.max_pool_size,
    )
    await db.open()
    try:
        yield db
    finally:
        await db.close()
