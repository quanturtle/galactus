import logging

from the_scraper import db
from the_scraper.html_cleaner import decompress

from supermercados.parsers import parse_api_response

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


async def run(source: str | None = None) -> int:
    """Parse unparsed API responses and insert into silver.products."""
    query = """
        SELECT id, source, endpoint, response_blob, fetched_at
        FROM bronze.api_responses
        WHERE parsed_at IS NULL
    """
    params = {}
    if source:
        query += " AND source = %(source)s"
        params["source"] = source
    query += " ORDER BY id"

    rows = await db.execute(query, params or None)

    if not rows:
        logger.info("No unparsed API responses found")
        return 0

    logger.info("Processing %d unparsed API responses", len(rows))

    silver_rows = []
    parsed_ids = []
    total_products = 0
    skipped = 0

    for row in rows:
        response_text = decompress(bytes(row["response_blob"]))
        results = parse_api_response(row["source"], response_text)

        if not results:
            skipped += 1
            parsed_ids.append(row["id"])
            continue

        for result in results:
            result["source"] = row["source"]
            result["scraped_at"] = row["fetched_at"]
            silver_rows.append(result)

        total_products += len(results)
        parsed_ids.append(row["id"])

        if len(silver_rows) >= BATCH_SIZE:
            await _flush(silver_rows, parsed_ids)
            silver_rows = []
            parsed_ids = []

    if silver_rows or parsed_ids:
        await _flush(silver_rows, parsed_ids)

    logger.info(
        "Done — %d products inserted into silver, %d API responses skipped",
        total_products, skipped,
    )
    return total_products


async def _flush(silver_rows: list[dict], parsed_ids: list[int]):
    if silver_rows:
        await db.bulk_insert("silver.products", silver_rows)
    if parsed_ids:
        await db.execute(
            "UPDATE bronze.api_responses SET parsed_at = NOW() WHERE id = ANY(%s)",
            [parsed_ids],
        )
