import logging

from the_scraper import db
from the_scraper.html_cleaner import decompress

from noticias.parsers import parse_snapshot
from noticias.transforms._common import _upsert_article

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000


async def run(source: str | None = None) -> int:
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

    rows = await db.execute(query, params or None)

    if not rows:
        logger.info("No unparsed snapshots found")
        return 0

    logger.info("Processing %d unparsed snapshots", len(rows))

    count = 0
    last_commit = 0
    skipped = 0
    parsed_ids: list[int] = []

    for row in rows:
        html = decompress(bytes(row["html_blob"]))
        try:
            article_dict = parse_snapshot(row["source"], html, row["url"])
        except Exception:
            logger.exception("Failed to parse snapshot %d (%s)", row["id"], row["url"])
            parsed_ids.append(row["id"])
            skipped += 1
            continue

        if article_dict:
            await _upsert_article(article_dict)
            count += 1
        else:
            skipped += 1

        parsed_ids.append(row["id"])

        if count - last_commit >= BATCH_SIZE:
            await db.execute(
                "UPDATE bronze.snapshots SET parsed_at = NOW() WHERE id = ANY(%s)",
                [parsed_ids],
            )
            logger.info("Snapshots: parsed %d, inserted %d into silver", len(parsed_ids), count)
            parsed_ids = []
            last_commit = count

    if parsed_ids:
        await db.execute(
            "UPDATE bronze.snapshots SET parsed_at = NOW() WHERE id = ANY(%s)",
            [parsed_ids],
        )

    logger.info("Snapshots: done — %d silver articles from %d snapshots, %d skipped", count, len(rows), skipped)
    return count
