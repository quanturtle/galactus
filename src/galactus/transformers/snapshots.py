"""Generic bronze.snapshots → silver transform.

Parameterized on a pydantic entity class (with async persist_many) plus the
domain's HTML transformer fn and the list of sources it covers.
"""

import logging
from typing import Any, Awaitable, Callable, Protocol

from galactus import db
from galactus.html_cleaner import decompress

logger = logging.getLogger(__name__)


class Persistable(Protocol):
    @classmethod
    def model_validate(cls, data: dict) -> Any: ...
    @classmethod
    def persist_many(
        cls, items: list, *, conn: Any,
    ) -> Awaitable[list[tuple[int, str, str]]]: ...


async def run(
    *,
    entity_cls: type[Persistable],
    transformer_fn: Callable[[str, str, str], dict | None],
    transformer_sources: list[str],
    chunk: int,
    source: str | None = None,
) -> int:
    query, params = _build_query(transformer_sources, source, chunk)
    total_rows = total_inserted = total_skipped = 0

    while True:
        rows = await db.execute(query, params)
        if not rows:
            break
        inserted, skipped = await _commit_chunk(rows, entity_cls, transformer_fn)
        total_rows += len(rows)
        total_inserted += inserted
        total_skipped += skipped
        logger.info(
            "snapshots: chunk of %d (total rows %d, inserted %d, skipped %d)",
            len(rows), total_rows, total_inserted, total_skipped,
        )

    if total_rows == 0:
        logger.info("snapshots: no unparsed rows")
    else:
        logger.info(
            "snapshots: done — %d inserted from %d rows, %d skipped",
            total_inserted, total_rows, total_skipped,
        )
    return total_inserted


def _build_query(transformer_sources: list[str], source: str | None, chunk: int) -> tuple[str, dict]:
    query = """
        SELECT id, source, url, html_blob
        FROM bronze.snapshots
        WHERE parsed_at IS NULL
          AND source = ANY(%(sources)s)
    """
    params: dict = {"sources": transformer_sources, "chunk": chunk}
    if source:
        query += " AND source = %(source)s"
        params["source"] = source
    query += " ORDER BY id LIMIT %(chunk)s"
    return query, params


def _transform_row(row: dict, transformer_fn: Callable) -> dict | None:
    html = decompress(bytes(row["html_blob"]))
    try:
        return transformer_fn(row["source"], html, row["url"])
    except Exception:
        logger.exception("Failed to transform snapshot %d (%s)", row["id"], row["url"])
        return None


def _build_entities(
    rows: list[dict], entity_cls: type, transformer_fn: Callable,
) -> tuple[list, int]:
    entities: list = []
    skipped = 0
    for row in rows:
        parsed = _transform_row(row, transformer_fn)
        if parsed is None:
            skipped += 1
            continue
        entities.append(entity_cls.model_validate({**parsed, "source": row["source"]}))
    return entities, skipped


async def _commit_chunk(
    rows: list[dict], entity_cls: type, transformer_fn: Callable,
) -> tuple[int, int]:
    entities, skipped = _build_entities(rows, entity_cls, transformer_fn)
    async with db.transaction() as conn:
        persisted = await entity_cls.persist_many(entities, conn=conn)
        await db.execute(
            "UPDATE bronze.snapshots SET parsed_at = NOW() WHERE id = ANY(%s)",
            [[r["id"] for r in rows]],
            conn=conn,
        )
    return len(persisted), skipped
