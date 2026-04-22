import logging

from the_scraper import db
from the_scraper.html_cleaner import decompress

from noticias.article import Article
from noticias.config import settings
from noticias.parsers import API_PARSERS, parse_api_response

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


def _parse_row(row) -> list[dict] | None:
    response_text = decompress(bytes(row["response_blob"]))
    try:
        return parse_api_response(row["source"], response_text)
    except Exception:
        logger.exception("Failed to parse api_response %d (%s)", row["id"], row["endpoint"])
        return None


async def _mark_parsed(rows, *, conn) -> None:
    await db.execute(
        "UPDATE bronze.api_responses SET parsed_at = NOW() WHERE id = ANY(%s)",
        [[r["id"] for r in rows]],
        conn=conn,
    )


def _build_articles(rows) -> tuple[list[Article], int]:
    skipped = 0
    articles: list[Article] = []
    for row in rows:
        parsed = _parse_row(row)
        if not parsed:
            skipped += 1
            continue
        articles.extend(Article.model_validate(p) for p in parsed)
    return articles, skipped


async def _commit_chunk(rows) -> tuple[int, int]:
    articles, skipped = _build_articles(rows)
    async with db.transaction() as conn:
        inserted = await Article.persist_many(articles, conn=conn)
        await _mark_parsed(rows, conn=conn)
    return inserted, skipped


async def run(source: str | None = None, *, chunk: int | None = None) -> int:
    chunk_size = chunk or settings.chunk_size
    query, params = _build_query(source, chunk_size)
    total_rows = total_articles = total_skipped = 0

    while True:
        rows = await db.execute(query, params)
        if not rows:
            break

        articles, skipped = await _commit_chunk(rows)
        total_articles += articles
        total_skipped += skipped
        total_rows += len(rows)
        logger.info(
            "API responses: chunk of %d (total rows %d, articles %d, skipped %d)",
            len(rows), total_rows, total_articles, total_skipped,
        )

    if total_rows == 0:
        logger.info("No unparsed API responses found")
    else:
        logger.info(
            "API responses: done — %d articles from %d responses, %d skipped",
            total_articles, total_rows, total_skipped,
        )
    return total_articles
