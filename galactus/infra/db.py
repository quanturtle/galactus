from collections.abc import AsyncIterator, Iterable
from typing import TypeVar

from sqlalchemy import select
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sql.base import Base

# mirror migrations/env.py: route bare postgresql:// to psycopg3
registry.register("postgresql", "sqlalchemy.dialects.postgresql.psycopg", "dialect")


M = TypeVar("M", bound=Base)


class Database:
    """Async SQLAlchemy-backed persistence.

    Owns one AsyncEngine and a sessionmaker; methods accept a mapped model class
    and one or many record instances of that class.
    """

    def __init__(
        self,
        database_url: str,
        pool_size: int = 5,
        max_overflow: int = 5,
    ) -> None:
        self._engine: AsyncEngine = create_async_engine(
            database_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
        )
        self._sessionmaker = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def open(self) -> None:
        # surface bad URLs / unreachable DB at startup, not lazily
        async with self._engine.connect():
            pass
        return

    async def close(self) -> None:
        await self._engine.dispose()
        return

    async def __aenter__(self) -> "Database":
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()
        return

    async def insert(
        self,
        records: M | Iterable[M],
        model: type[M],
        conflict_columns: Iterable[str],
        exclude_columns: Iterable[str] = (),
    ) -> None:
        """Idempotent insert: ON CONFLICT (conflict_columns) DO NOTHING.

        `exclude_columns` lists model attributes to drop from the row dict
        (typically server-managed columns like surrogate ids and auto timestamps).
        """
        if isinstance(records, Base):
            records = [records]
        excluded = frozenset(exclude_columns)
        rows = [r.to_dict(excluded) for r in records]
        if not rows:
            return
        stmt = pg_insert(model).on_conflict_do_nothing(
            index_elements=list(conflict_columns),
        )
        async with self._sessionmaker() as session:
            await session.execute(stmt, rows)
            await session.commit()
        return

    async def upsert(
        self,
        records: M | Iterable[M],
        model: type[M],
        conflict_columns: Iterable[str],
        exclude_columns: Iterable[str] = (),
    ) -> None:
        """Idempotent upsert: ON CONFLICT (conflict_columns) DO UPDATE.

        Every column except `conflict_columns` and `exclude_columns` is
        overwritten with the EXCLUDED.* row value.
        """
        if isinstance(records, Base):
            records = [records]
        excluded = frozenset(exclude_columns)
        rows = [r.to_dict(excluded) for r in records]
        if not rows:
            return
        conflict = list(conflict_columns)
        immutable = excluded | set(conflict)
        stmt = pg_insert(model)
        update_set = {c.name: c for c in stmt.excluded if c.name not in immutable}
        stmt = stmt.on_conflict_do_update(
            index_elements=conflict,
            set_=update_set,
        )
        async with self._sessionmaker() as session:
            await session.execute(stmt, rows)
            await session.commit()
        return

    async def load_for_source(self, model: type[M], source: str) -> AsyncIterator[M]:
        """Stream all bronze rows for `source`, ordered by created_at then bronze_id.

        Server-side cursor; the transaction is held for the iterator's lifetime.
        Idempotent re-scan model: callers re-read the full source on every run and
        rely on silver upsert (source, source_url) for dedup.
        """
        stmt = (
            select(model).where(model.source == source).order_by(model.created_at, model.bronze_id)
        )
        async with self._sessionmaker() as session:
            result = await session.stream_scalars(stmt)
            async for record in result:
                yield record
