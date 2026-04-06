import logging

from the_scraper.html_cleaner import decompress

from supermercados import db
from supermercados.parsers import parse_snapshot

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def run(source: str | None = None):
    """Parse unparsed snapshots and insert into silver.products."""
    query = """
        SELECT id, source, url, html_blob, fetched_at
        FROM bronze.snapshots
        WHERE parsed_at IS NULL
    """
    params = {}
    if source:
        query += " AND source = %(source)s"
        params["source"] = source
    query += " ORDER BY id"

    rows = db.execute(query, params or None)

    if not rows:
        logger.info("No unparsed snapshots found")
        return

    logger.info("Processing %d unparsed snapshots", len(rows))

    silver_rows = []
    parsed_ids = []
    skipped = 0

    for row in rows:
        html = decompress(bytes(row["html_blob"]))
        result = parse_snapshot(row["source"], html, row["url"])

        if result is None:
            skipped += 1
            parsed_ids.append(row["id"])
            continue

        result["source"] = row["source"]
        result["scraped_at"] = row["fetched_at"]
        silver_rows.append(result)
        parsed_ids.append(row["id"])

        if len(silver_rows) >= BATCH_SIZE:
            _flush(silver_rows, parsed_ids)
            silver_rows = []
            parsed_ids = []

    if silver_rows or parsed_ids:
        _flush(silver_rows, parsed_ids)

    logger.info(
        "Done — %d products inserted into silver, %d snapshots skipped",
        len(rows) - skipped, skipped,
    )


def _flush(silver_rows: list[dict], parsed_ids: list[int]):
    if silver_rows:
        db.bulk_insert("silver.products", silver_rows)
    if parsed_ids:
        db.execute(
            "UPDATE bronze.snapshots SET parsed_at = NOW() WHERE id = ANY(%s)",
            [parsed_ids],
        )
