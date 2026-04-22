import logging

from the_scraper import db
from the_scraper.html_cleaner import decompress

from noticias.parsers import API_PARSERS, parse_api_response
from noticias.transforms._common import _upsert_article

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000


async def run(source: str | None = None) -> int:
    query = """
        SELECT id, source, endpoint, response_blob, fetched_at
        FROM bronze.api_responses
        WHERE parsed_at IS NULL
          AND source = ANY(%(sources)s)
    """
    params: dict = {"sources": list(API_PARSERS)}
    if source:
        query += " AND source = %(source)s"
        params["source"] = source
    query += " ORDER BY id"

    rows = await db.execute(query, params)

    if not rows:
        logger.info("No unparsed API responses found")
        return 0

    logger.info("Processing %d unparsed API responses", len(rows))

    count = 0
    last_commit = 0
    skipped = 0
    parsed_ids: list[int] = []

    for row in rows:
        response_text = decompress(bytes(row["response_blob"]))
        try:
            articles = parse_api_response(row["source"], response_text)
        except Exception:
            logger.exception("Failed to parse api_response %d (%s)", row["id"], row["endpoint"])
            parsed_ids.append(row["id"])
            skipped += 1
            continue

        if not articles:
            skipped += 1
        else:
            for article_dict in articles:
                await _upsert_article(article_dict)
                count += 1

        parsed_ids.append(row["id"])

        if count - last_commit >= BATCH_SIZE:
            await db.execute(
                "UPDATE bronze.api_responses SET parsed_at = NOW() WHERE id = ANY(%s)",
                [parsed_ids],
            )
            logger.info("API responses: parsed %d, inserted %d into silver", len(parsed_ids), count)
            parsed_ids = []
            last_commit = count

    if parsed_ids:
        await db.execute(
            "UPDATE bronze.api_responses SET parsed_at = NOW() WHERE id = ANY(%s)",
            [parsed_ids],
        )

    logger.info("API responses: done — %d silver articles from %d responses, %d skipped", count, len(rows), skipped)
    return count
