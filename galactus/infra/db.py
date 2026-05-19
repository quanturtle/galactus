import logging
from collections.abc import AsyncIterator, Iterable
from types import TracebackType
from typing import Any, TypeVar

from sqlalchemy import func, insert, select
from sqlalchemy.dialects import registry
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from zstandard import ZstdCompressor, ZstdDecompressor

from galactus.core.errors import DatabaseError
from sql.base import Base

# mirror migrations/env.py: route bare postgresql:// to psycopg3
registry.register("postgresql", "sqlalchemy.dialects.postgresql.psycopg", "dialect")

logger = logging.getLogger(__name__)


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
        **engine_kwargs: Any,
    ) -> None:
        self._engine: AsyncEngine = create_async_engine(
            database_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            **engine_kwargs,
        )
        self._sessionmaker = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        self.compressor = ZstdCompressor(level=6)
        self.decompressor = ZstdDecompressor()
        # log host+db only — never user or password
        url = make_url(database_url)
        logger.info(
            "Database initialized (host=%s, db=%s, pool_size=%s, max_overflow=%s)",
            url.host,
            url.database,
            pool_size,
            max_overflow,
        )

    def compress(self, text: str) -> bytes:
        """zstd-compress a UTF-8 string for BYTEA storage."""
        return self.compressor.compress(text.encode("utf-8"))

    def decompress(self, blob: bytes) -> str:
        """Decompress a zstd blob back to a UTF-8 string."""
        return self.decompressor.decompress(blob).decode("utf-8")

    async def open(self) -> None:
        # surface bad URLs / unreachable DB at startup, not lazily
        try:
            async with self._engine.connect():
                pass
        except SQLAlchemyError as exc:
            raise DatabaseError("cannot connect to database") from exc
        logger.info("Database connection verified")
        return

    async def close(self) -> None:
        await self._engine.dispose()
        return

    async def __aenter__(self) -> "Database":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
        return

    async def insert(self, records: M | Iterable[M], model: type[M]) -> None:
        """Bulk-insert one or many records of `model`.

        Columns left unset (None) on every record — surrogate ids, server-filled
        timestamps — are dropped from the row dicts so the database applies its
        own defaults; the remaining columns are identical across rows.
        """
        if isinstance(records, Base):
            records = [records]
        rows = [r.to_dict() for r in records]
        if not rows:
            return
        unset = {k for k in rows[0] if all(row[k] is None for row in rows)}
        rows = [{k: v for k, v in row.items() if k not in unset} for row in rows]
        try:
            async with self._sessionmaker() as session:
                await session.execute(insert(model), rows)
                await session.commit()
        except SQLAlchemyError as exc:
            raise DatabaseError(f"{model.__name__} insert failed") from exc
        logger.info("inserted %d %s rows", len(rows), model.__name__)
        return

    async def load_visited_requests(
        self,
        model: type[M],
        source: str,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Return (request_url, request_params) for `source` captured since UTC midnight (2xx).

        Restricted to 2xx responses — a 4xx/5xx from earlier today should be
        retried on the next run, not treated as "already visited". The cutoff
        is computed server-side so client clock drift doesn't matter.
        """
        today_start = func.date_trunc("day", func.timezone("UTC", func.now()))
        stmt = (
            select(model.request_url, model.request_params)
            .where(
                model.source == source,
                model.created_at >= today_start,
                model.status_code >= 200,
                model.status_code < 300,
            )
            .distinct()
        )
        try:
            async with self._sessionmaker() as session:
                result = await session.execute(stmt)
                rows = result.all()
        except SQLAlchemyError as exc:
            raise DatabaseError(
                f"loading visited requests for {model.__name__} source {source!r} failed"
            ) from exc
        return [(row[0], dict(row[1] or {})) for row in rows]

    async def stream_unparsed(
        self,
        bronze_model: type[M],
        silver_model: type[Base],
        source: str,
        chunk_size: int = 100,
    ) -> AsyncIterator[M]:
        """Yield bronze rows for `source` that no silver row references yet.

        A bronze row counts as parsed once any silver row carries its
        (source, bronze_id) — one bronze row may yield many silver entities.
        Ordered by created_at then id. Rows are fetched from the server
        in chunks of `chunk_size`, so memory stays bounded regardless of how
        many bronze rows remain unparsed. Safe to re-run: bronze rows whose
        silver already committed are not returned on the next pass.
        """
        already_parsed = (
            select(silver_model.bronze_id)
            .where(silver_model.source == source)
            .where(silver_model.bronze_id == bronze_model.id)
            .exists()
        )
        stmt = (
            select(bronze_model)
            .where(bronze_model.source == source, ~already_parsed)
            .order_by(bronze_model.created_at, bronze_model.id)
            .execution_options(yield_per=chunk_size)
        )
        try:
            async with self._sessionmaker() as session:
                result = await session.stream_scalars(stmt)
                async for row in result:
                    yield row
        except SQLAlchemyError as exc:
            raise DatabaseError(
                f"streaming unparsed {bronze_model.__name__} for source {source!r} failed"
            ) from exc
