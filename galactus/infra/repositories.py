from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from typing import Any

from galactus.core.records import ParsedRecord, RawRecord
from galactus.core.types import BronzeId, SourceName
from galactus.infra.db import Database


@dataclass(frozen=True, slots=True)
class RepoConfig:
    """Per-instance configuration for PsycopgRepo.

    The same class implements BronzeRepo, SilverRepo, and GoldRepo Protocols
    structurally; what differs between layers is which table is targeted,
    which columns form the conflict key, and which methods the caller invokes.
    """

    table: str
    conflict_keys: tuple[str, ...] = ()


class PsycopgRepo:
    """One repository implementation, configured per use.

    The same class can satisfy core.BronzeRepo, core.SilverRepo, or core.GoldRepo
    Protocols depending on which methods the caller invokes. Layer-specific
    behavior is parameterized via RepoConfig — not subclassed.

    Concrete SQL is left as TODOs to fill in once the bronze/silver/gold
    schemas are finalized in migrations/.
    """

    def __init__(self, db: Database, config: RepoConfig) -> None:
        self.db = db
        self.config = config

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
