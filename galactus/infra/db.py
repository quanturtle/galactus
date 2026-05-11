from collections.abc import Iterable
from typing import TypeVar

from sqlalchemy import select
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from galactus.core.errors import DatabaseError
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
        try:
            async with self._engine.connect():
                pass
        except SQLAlchemyError as exc:
            raise DatabaseError("cannot connect to database") from exc
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
        conflict_columns: Iterable[str] = (),
        exclude_columns: Iterable[str] = (),
    ) -> None:
        """Insert rows. With `conflict_columns`, collisions are skipped (ON CONFLICT DO NOTHING).

        `exclude_columns` lists model attributes to drop from the row dict
        (typically server-managed columns like surrogate ids and auto timestamps).
        """
        if isinstance(records, Base):
            records = [records]
        excluded = frozenset(exclude_columns)
        rows = [r.to_dict(excluded) for r in records]
        if not rows:
            return
        stmt = pg_insert(model)
        conflict = list(conflict_columns)
        if conflict:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict)
        try:
            async with self._sessionmaker() as session:
                await session.execute(stmt, rows)
                await session.commit()
        except SQLAlchemyError as exc:
            raise DatabaseError(f"{model.__name__} insert failed") from exc
        return

    async def load_unparsed(
        self,
        bronze_model: type[M],
        silver_model: type[Base],
        source: str,
    ) -> list[M]:
        """Return the bronze rows for `source` that no silver row references yet.

        A bronze row counts as parsed once any silver row carries its
        (source, bronze_id) — one bronze row may yield many silver entities.
        Ordered by created_at then bronze_id. Safe to re-run: bronze rows whose
        silver already committed are not returned on the next pass.
        """
        already_parsed = (
            select(silver_model.bronze_id)
            .where(silver_model.source == source)
            .where(silver_model.bronze_id == bronze_model.bronze_id)
            .exists()
        )
        stmt = (
            select(bronze_model)
            .where(bronze_model.source == source, ~already_parsed)
            .order_by(bronze_model.created_at, bronze_model.bronze_id)
        )
        try:
            async with self._sessionmaker() as session:
                result = await session.scalars(stmt)
                return list(result.all())
        except SQLAlchemyError as exc:
            raise DatabaseError(
                f"loading unparsed {bronze_model.__name__} for source {source!r} failed"
            ) from exc
