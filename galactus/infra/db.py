from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from galactus.config import DatabaseConfig
from galactus.core.records import ParsedRecord, RawRecord
from galactus.core.types import BronzeId, SourceName


class Database:
    """Async psycopg-backed persistence.

    Owns a single AsyncConnectionPool and exposes domain-flavored persistence
    ops (`insert`, `upsert`, `load_unparsed`, `mark_parsed`); table names and
    conflict keys are passed per call rather than carried per instance.

    Concrete SQL is left as TODOs to fill in once the bronze/silver schemas
    are finalized in migrations/.
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

    async def insert(
        self,
        records: RawRecord | Iterable[RawRecord],
        *,
        table: str,
    ) -> None:
        """Idempotent insert of one or many RawRecords into `table`.

        Bronze conflict policy is fixed by table schema (no per-call
        conflict_keys parameter, intentionally). Accepts a single record or any
        iterable.
        """
        raise NotImplementedError

    async def upsert(
        self,
        records: ParsedRecord | Iterable[ParsedRecord],
        *,
        table: str,
        conflict_keys: tuple[str, ...],
    ) -> None:
        """Idempotent upsert of one or many ParsedRecords into `table`.

        INSERT ... ON CONFLICT (conflict_keys) DO UPDATE. Accepts a single
        record or any iterable.
        """
        raise NotImplementedError

    async def load_unparsed(
        self,
        source: SourceName,
        *,
        table: str,
    ) -> AsyncIterator[RawRecord]:
        """Stream RawRecords from `table` that have not yet been marked parsed."""
        raise NotImplementedError
        yield  # pragma: no cover

    async def mark_parsed(self, ids: Iterable[BronzeId], *, table: str) -> None:
        """Flag the given bronze rows in `table` as parsed."""
        raise NotImplementedError

    # private pool helpers — used by the public methods above
    async def _fetch_all(self, sql: str, params: Sequence[Any] | None = None) -> list[dict]:
        async with (
            self._pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cur,
        ):
            await cur.execute(sql, params)
            return await cur.fetchall()

    async def _execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
        return

    async def _execute_many(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
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
