"""Generic bronze.api_responses → silver transform.

Parameterized on a pydantic entity class (with async persist_many) plus the
domain's API parser fn and the list of sources it covers.
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
    def persist_many(cls, items: list, *, conn: Any) -> Awaitable[int]: ...


async def run(
    *,
    entity_cls: type[Persistable],
    parser_fn: Callable[[str, str], list[dict]],
    parser_sources: list[str],
    chunk: int,
    source: str | None = None,
) -> int:
    query, params = _build_query(parser_sources, source, chunk)
    total_rows = total_inserted = total_skipped = 0

    while True:
        rows = await db.execute(query, params)
        if not rows:
            break
        inserted, skipped = await _commit_chunk(rows, entity_cls, parser_fn)
        total_rows += len(rows)
        total_inserted += inserted
        total_skipped += skipped
        logger.info(
            "api_responses: chunk of %d (total rows %d, inserted %d, skipped %d)",
            len(rows), total_rows, total_inserted, total_skipped,
        )

    if total_rows == 0:
        logger.info("api_responses: no unparsed rows")
    else:
        logger.info(
            "api_responses: done — %d inserted from %d rows, %d skipped",
            total_inserted, total_rows, total_skipped,
        )
    return total_inserted


def _build_query(parser_sources: list[str], source: str | None, chunk: int) -> tuple[str, dict]:
    query = """
        SELECT id, source, endpoint, response_blob
        FROM bronze.api_responses
        WHERE parsed_at IS NULL
          AND source = ANY(%(sources)s)
    """
    params: dict = {"sources": parser_sources, "chunk": chunk}
    if source:
        query += " AND source = %(source)s"
        params["source"] = source
    query += " ORDER BY id LIMIT %(chunk)s"
    return query, params


def _parse_row(row: dict, parser_fn: Callable) -> list[dict] | None:
    response_text = decompress(bytes(row["response_blob"]))
    try:
        return parser_fn(row["source"], response_text)
    except Exception:
        logger.exception("Failed to parse api_response %d (%s)", row["id"], row["endpoint"])
        return None


def _build_entities(
    rows: list[dict], entity_cls: type, parser_fn: Callable,
) -> tuple[list, int]:
    entities: list = []
    skipped = 0
    for row in rows:
        parsed = _parse_row(row, parser_fn)
        if not parsed:
            skipped += 1
            continue
        entities.extend(
            entity_cls.model_validate({**item, "source": row["source"]})
            for item in parsed
        )
    return entities, skipped


async def _commit_chunk(
    rows: list[dict], entity_cls: type, parser_fn: Callable,
) -> tuple[int, int]:
    entities, skipped = _build_entities(rows, entity_cls, parser_fn)
    async with db.transaction() as conn:
        inserted = await entity_cls.persist_many(entities, conn=conn)
        await db.execute(
            "UPDATE bronze.api_responses SET parsed_at = NOW() WHERE id = ANY(%s)",
            [[r["id"] for r in rows]],
            conn=conn,
        )
    return inserted, skipped
