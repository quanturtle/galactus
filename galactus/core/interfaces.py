from collections.abc import AsyncIterator, Iterable, Mapping
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from galactus.core.records import ParsedRecord, RawRecord
from galactus.core.types import BronzeId, SourceName


@runtime_checkable
class HttpClient(Protocol):
    """Minimal HTTP client seam used by scrapers."""

    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> "HttpResponse": ...

    async def aclose(self) -> None: ...


@runtime_checkable
class HttpResponse(Protocol):
    status_code: int
    headers: Mapping[str, str]
    content: bytes
    text: str

    def json(self) -> Any: ...


@runtime_checkable
class BronzeRepo(Protocol):
    """Persistence seam for raw fetched records."""

    async def store(self, record: RawRecord) -> BronzeId:
        """Idempotent insert keyed by (source, source_url, fetched_at_date)."""
        ...

    async def load_unparsed(self, source: SourceName) -> AsyncIterator[RawRecord]: ...

    async def mark_parsed(self, ids: Iterable[BronzeId]) -> None: ...


@runtime_checkable
class SilverRepo(Protocol):
    """Persistence seam for parsed entities."""

    async def upsert_many(self, records: Iterable[ParsedRecord]) -> None:
        """Idempotent on (source, source_url)."""
        ...


@runtime_checkable
class GoldRepo(Protocol):
    """Persistence seam for aggregated/derived data (load stage). Stubbed for now."""

    async def write(self, payload: Any) -> None: ...


@runtime_checkable
class BlobStore(Protocol):
    """Object store seam (e.g. S3) for large payloads such as raw HTML or images."""

    async def put(self, key: str, body: bytes, content_type: str) -> str: ...

    async def get(self, key: str) -> bytes: ...


@runtime_checkable
class Clock(Protocol):
    """Time seam — pass instead of calling datetime.now() directly in domain code."""

    def now(self) -> datetime: ...


@runtime_checkable
class PipelineStage(Protocol):
    """One pipeline stage. Pipeline holds an ordered list of these and dispatches by `name`."""

    name: str

    async def run(self) -> None: ...
