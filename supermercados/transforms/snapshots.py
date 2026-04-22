import logging

from the_scraper import db
from the_scraper.html_cleaner import decompress

from supermercados.parsers import HTML_PARSERS, parse_snapshot
from supermercados.product import Product

logger = logging.getLogger(__name__)

CHUNK = 500


def _build_query(source: str | None) -> tuple[str, dict]:
    query = """
        SELECT id, source, url, html_blob, fetched_at
        FROM bronze.snapshots
        WHERE parsed_at IS NULL
          AND source = ANY(%(sources)s)
    """
    params: dict = {"sources": list(HTML_PARSERS), "chunk": CHUNK}
    if source:
        query += " AND source = %(source)s"
        params["source"] = source
    query += " ORDER BY id LIMIT %(chunk)s"
    return query, params


def _parse_row(row) -> Product | None:
    html = decompress(bytes(row["html_blob"]))
    result = parse_snapshot(row["source"], html, row["url"])
    if result is None:
        return None
    return Product.model_validate(
        {**result, "source": row["source"], "scraped_at": row["fetched_at"]}
    )


async def _mark_parsed(rows) -> None:
    await db.execute(
        "UPDATE bronze.snapshots SET parsed_at = NOW() WHERE id = ANY(%s)",
        [[r["id"] for r in rows]],
    )


def _chunk_products(rows) -> tuple[list[Product], int]:
    products: list[Product] = []
    skipped = 0
    for row in rows:
        parsed = _parse_row(row)
        if parsed is None:
            skipped += 1
        else:
            products.append(parsed)
    return products, skipped


async def run(source: str | None = None) -> int:
    """Parse unparsed snapshots and insert into silver.products."""
    query, params = _build_query(source)
    total_rows = total_products = total_skipped = 0

    while True:
        rows = await db.execute(query, params)
        if not rows:
            break

        products, skipped = _chunk_products(rows)
        inserted = await Product.persist_many(products)
        await _mark_parsed(rows)
        total_products += inserted
        total_skipped += skipped
        total_rows += len(rows)
        logger.info(
            "Snapshots: chunk of %d (total rows %d, products %d, skipped %d)",
            len(rows), total_rows, total_products, total_skipped,
        )

    if total_rows == 0:
        logger.info("No unparsed snapshots found")
    else:
        logger.info(
            "Done — %d products inserted into silver from %d snapshots, %d skipped",
            total_products, total_rows, total_skipped,
        )
    return total_products
