from collections.abc import AsyncIterator, Iterable
from typing import Any

from galactus.core.records import ParsedRecord, RawRecord
from galactus.core.types import BronzeId, SourceName
from galactus.infra.db import Database


class PsycopgRepo:
    """One repository implementation, configured per use.

    The same class can satisfy core.BronzeRepo, core.SilverRepo, or core.GoldRepo
    Protocols depending on which methods the caller invokes. Layer-specific
    behavior is parameterized via `table` and `conflict_keys` — not subclassed.

    Concrete SQL is left as TODOs to fill in once the bronze/silver/gold
    schemas are finalized in migrations/.
    """

    def __init__(self, db: Database, *, table: str, conflict_keys: tuple[str, ...] = ()) -> None:
        self.db = db
        self.table = table
        self.conflict_keys = conflict_keys

    # bronze methods
    async def store(self, record: RawRecord) -> BronzeId:
        # idempotent insert: ON CONFLICT (conflict_keys) DO NOTHING RETURNING id
        raise NotImplementedError

    async def load_unparsed(self, source: SourceName) -> AsyncIterator[RawRecord]:
        raise NotImplementedError
        yield  # pragma: no cover

    async def mark_parsed(self, ids: Iterable[BronzeId]) -> None:
        raise NotImplementedError

    # silver methods
    async def upsert_many(self, records: Iterable[ParsedRecord]) -> None:
        # idempotent on conflict_keys — INSERT ... ON CONFLICT DO UPDATE
        raise NotImplementedError

    # gold methods
    async def write(self, payload: Any) -> None:
        raise NotImplementedError
