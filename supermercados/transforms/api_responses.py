import logging

from galactus import db
from galactus.html_cleaner import decompress

from supermercados.config import settings
from supermercados.parsers import API_PARSERS, parse_api_response
from supermercados.product import Product

logger = logging.getLogger(__name__)


def _build_query(source: str | None, chunk: int) -> tuple[str, dict]:
    query = """
        SELECT id, source, endpoint, response_blob
        FROM bronze.api_responses
        WHERE parsed_at IS NULL
          AND source = ANY(%(sources)s)
    """
    params: dict = {"sources": list(API_PARSERS), "chunk": chunk}
    if source:
        query += " AND source = %(source)s"
        params["source"] = source
    query += " ORDER BY id LIMIT %(chunk)s"
    return query, params


def _parse_row(row) -> list[Product]:
    response_text = decompress(bytes(row["response_blob"]))
    results = parse_api_response(row["source"], response_text)
    return [
        Product.model_validate({**r, "source": row["source"]})
        for r in results
    ]


async def _mark_parsed(rows, *, conn) -> None:
    await db.execute(
        "UPDATE bronze.api_responses SET parsed_at = NOW() WHERE id = ANY(%s)",
        [[r["id"] for r in rows]],
        conn=conn,
    )


def _build_products(rows) -> tuple[list[Product], int]:
    products: list[Product] = []
    skipped = 0
    for row in rows:
        parsed = _parse_row(row)
        if parsed:
            products.extend(parsed)
        else:
            skipped += 1
    return products, skipped


async def _commit_chunk(rows) -> tuple[int, int]:
    products, skipped = _build_products(rows)
    async with db.transaction() as conn:
        inserted = await Product.persist_many(products, conn=conn)
        await _mark_parsed(rows, conn=conn)
    return inserted, skipped


async def run(source: str | None = None, *, chunk: int | None = None) -> int:
    """Parse unparsed API responses and insert into silver.products."""
    chunk_size = chunk or settings.chunk_size
    query, params = _build_query(source, chunk_size)
    total_rows = total_products = total_skipped = 0

    while True:
        rows = await db.execute(query, params)
        if not rows:
            break

        products, skipped = await _commit_chunk(rows)
        total_products += products
        total_skipped += skipped
        total_rows += len(rows)
        logger.info(
            "API responses: chunk of %d (total rows %d, products %d, skipped %d)",
            len(rows), total_rows, total_products, total_skipped,
        )

    if total_rows == 0:
        logger.info("No unparsed API responses found")
    else:
        logger.info(
            "Done — %d products inserted into silver from %d responses, %d skipped",
            total_products, total_rows, total_skipped,
        )
    return total_products
