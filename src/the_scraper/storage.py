"""Storage protocols and psycopg3 implementations for the scraper framework.

Protocols decouple scrapers from the database layer.  The Psycopg*
classes implement those protocols against the shared async connection pool.
"""

from typing import Protocol, runtime_checkable

from the_scraper import db


# ── Protocols ────────────────────────────────────────────────────────

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


# ── Implementations ──────────────────────────────────────────────────

class PsycopgApiStorage:

    def __init__(self):
        self._pending: list[dict] = []

    async def load_today_endpoints(self, source: str) -> set[str]:
        rows = await db.execute(
            "SELECT endpoint FROM bronze.api_responses "
            "WHERE source = %(source)s AND fetch_date = CURRENT_DATE",
            {"source": source},
        )
        return {r["endpoint"] for r in rows}

    async def store_response(
        self, source: str, endpoint: str, page_params: dict, response_blob: bytes,
    ) -> None:
        self._pending.append({
            "source": source,
            "endpoint": endpoint,
            "page_params": page_params,
            "response_blob": response_blob,
        })
        if len(self._pending) >= 50:
            await self.flush()

    async def flush(self) -> None:
        if self._pending:
            await db.bulk_insert("bronze.api_responses", self._pending)
            self._pending = []


class PsycopgSnapshotStorage:

    async def load_today_urls(self, source: str) -> set[str]:
        rows = await db.execute(
            "SELECT url FROM bronze.snapshots "
            "WHERE source = %(source)s AND fetch_date = CURRENT_DATE",
            {"source": source},
        )
        return {r["url"] for r in rows}

    async def store_snapshot(
        self, source: str, url: str, html_blob: bytes, content_hash: str | None = None,
    ) -> bool:
        if content_hash:
            existing = await self.get_content_hashes(source, [url])
            if existing.get(url) == content_hash:
                return False
        row = {"source": source, "url": url, "html_blob": html_blob}
        if content_hash:
            row["content_hash"] = content_hash
        await db.bulk_insert("bronze.snapshots", [row])
        return True

    async def get_content_hashes(self, source: str, urls: list[str]) -> dict[str, str]:
        if not urls:
            return {}
        rows = await db.execute(
            "SELECT DISTINCT ON (url) url, content_hash "
            "FROM bronze.snapshots "
            "WHERE source = %(source)s AND url = ANY(%(urls)s) "
            "AND content_hash IS NOT NULL "
            "ORDER BY url, fetch_date DESC",
            {"source": source, "urls": urls},
        )
        return {r["url"]: r["content_hash"] for r in rows}

    async def flush(self) -> None:
        pass  # each store_snapshot auto-commits via bulk_insert
