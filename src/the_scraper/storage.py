"""Storage protocols that decouple scrapers from the database layer.

Each project implements these protocols with its own DB technology
(e.g. psycopg sync, SQLAlchemy async).
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class ApiStorage(Protocol):
    """Protocol for storing paginated API responses."""

    async def load_today_endpoints(self, source: str) -> set[str]:
        """Return endpoints already fetched today for this source."""
        ...

    async def store_response(
        self,
        source: str,
        endpoint: str,
        page_params: dict,
        response_blob: bytes,
    ) -> None:
        """Store a single compressed API response."""
        ...

    async def flush(self) -> None:
        """Commit any pending writes."""
        ...


@runtime_checkable
class SnapshotStorage(Protocol):
    """Protocol for storing BFS crawler HTML snapshots."""

    async def load_today_urls(self, source: str) -> set[str]:
        """Return URLs already snapshotted today for this source."""
        ...

    async def store_snapshot(
        self,
        source: str,
        url: str,
        html_blob: bytes,
        content_hash: str | None = None,
    ) -> bool:
        """Store a snapshot. Return True if stored, False if skipped."""
        ...

    async def get_content_hashes(
        self, source: str, urls: list[str]
    ) -> dict[str, str]:
        """Return most recent content_hash for each URL. Empty dict if unsupported."""
        ...

    async def flush(self) -> None:
        """Commit any pending writes."""
        ...
