"""Storage protocol implementations backed by SQLAlchemy async sessions."""

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from noticias.db.bronze import ApiResponse, Snapshot


class SQLAlchemyApiStorage:

    def __init__(self, session: AsyncSession):
        self._session = session

    async def load_today_endpoints(self, source: str) -> set[str]:
        result = await self._session.execute(
            select(ApiResponse.endpoint).where(
                ApiResponse.source == source,
                ApiResponse.fetch_date == text("CURRENT_DATE"),
            )
        )
        return {row[0] for row in result.all()}

    async def store_response(
        self, source: str, endpoint: str, page_params: dict, response_blob: bytes,
    ) -> None:
        stmt = (
            insert(ApiResponse)
            .values(
                source=source,
                endpoint=endpoint,
                page_params=page_params,
                response_blob=response_blob,
            )
            .on_conflict_do_nothing(constraint="uq_api_responses_source_endpoint_date")
        )
        await self._session.execute(stmt)

    async def flush(self) -> None:
        await self._session.commit()


class SQLAlchemySnapshotStorage:

    def __init__(self, session: AsyncSession):
        self._session = session

    async def load_today_urls(self, source: str) -> set[str]:
        result = await self._session.execute(
            select(Snapshot.url).where(
                Snapshot.source == source,
                Snapshot.fetch_date == text("CURRENT_DATE"),
            )
        )
        return {row[0] for row in result.all()}

    async def store_snapshot(
        self, source: str, url: str, html_blob: bytes, content_hash: str | None = None,
    ) -> bool:
        stmt = (
            insert(Snapshot)
            .values(source=source, url=url, html_blob=html_blob)
            .on_conflict_do_nothing(constraint="uq_snapshots_source_url_date")
        )
        result = await self._session.execute(stmt)
        return result.rowcount > 0

    async def get_content_hashes(self, source: str, urls: list[str]) -> dict[str, str]:
        return {}  # noticias does not use content hashing

    async def flush(self) -> None:
        await self._session.commit()
