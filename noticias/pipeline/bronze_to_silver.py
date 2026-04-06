"""Parse raw bronze data and insert into silver.articles.

Reads from bronze.snapshots and bronze.api_responses where parsed_at IS NULL,
runs per-source parsers, enriches (dates, keywords, word count), and upserts
into silver.articles + silver.article_images.  Marks bronze rows as parsed.
"""

import json
import logging

from dateutil import parser as dateparser

from the_scraper import db
from the_scraper.html_cleaner import decompress

from noticias.parsers import API_PARSERS, HTML_PARSERS, parse_api_response, parse_snapshot

logger = logging.getLogger(__name__)

SILVER_BATCH_SIZE = 1000


def _parse_date(raw):
    if not raw:
        return None
    try:
        return dateparser.parse(raw)
    except (ValueError, OverflowError):
        return None


def _extract_keywords(title):
    if not title:
        return None
    words = [w.lower().strip(".,;:!?¿¡\"'()") for w in title.split()]
    keywords = [w for w in words if len(w) > 2]
    return keywords if keywords else None


def _count_words(body):
    if not body:
        return None
    return len(body.split())


async def _upsert_article(article_dict: dict) -> None:
    published = _parse_date(article_dict.get("published_at"))
    word_count = _count_words(article_dict.get("body"))
    keywords = _extract_keywords(article_dict.get("title"))

    rows = await db.execute(
        """
        INSERT INTO silver.articles
            (source, source_url, title, subtitle, body, author,
             published_at, section, image_url, word_count, keywords)
        VALUES
            (%(source)s, %(source_url)s, %(title)s, %(subtitle)s, %(body)s,
             %(author)s, %(published_at)s, %(section)s, %(image_url)s,
             %(word_count)s, %(keywords)s)
        ON CONFLICT ON CONSTRAINT uq_silver_source_url
        DO UPDATE SET
            title       = EXCLUDED.title,
            subtitle    = EXCLUDED.subtitle,
            body        = EXCLUDED.body,
            author      = EXCLUDED.author,
            published_at = EXCLUDED.published_at,
            section     = EXCLUDED.section,
            image_url   = EXCLUDED.image_url,
            word_count  = EXCLUDED.word_count,
            keywords    = EXCLUDED.keywords,
            processed_at = now()
        RETURNING id
        """,
        {
            "source": article_dict["source"],
            "source_url": article_dict["source_url"],
            "title": article_dict.get("title"),
            "subtitle": article_dict.get("subtitle"),
            "body": article_dict.get("body"),
            "author": article_dict.get("author"),
            "published_at": published,
            "section": article_dict.get("section"),
            "image_url": article_dict.get("image_url"),
            "word_count": word_count,
            "keywords": keywords,
        },
    )
    silver_id = rows[0]["id"]

    image_urls = []
    raw_image_urls = article_dict.get("image_urls")
    if raw_image_urls:
        try:
            image_urls = json.loads(raw_image_urls) if isinstance(raw_image_urls, str) else raw_image_urls
        except (json.JSONDecodeError, TypeError):
            pass
    if not image_urls and article_dict.get("image_url"):
        image_urls = [article_dict["image_url"]]

    for idx, img_url in enumerate(image_urls):
        role = "hero" if idx == 0 and img_url == article_dict.get("image_url") else "body"
        await db.execute(
            """
            INSERT INTO silver.article_images
                (silver_article_id, image_url, image_role, ordinal)
            VALUES (%(silver_article_id)s, %(image_url)s, %(image_role)s, %(ordinal)s)
            ON CONFLICT ON CONSTRAINT uq_silver_article_image DO NOTHING
            """,
            {
                "silver_article_id": silver_id,
                "image_url": img_url,
                "image_role": role,
                "ordinal": idx,
            },
        )


async def parse_snapshots(source: str | None = None) -> int:
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

        if count - last_commit >= SILVER_BATCH_SIZE:
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


async def parse_api_responses(source: str | None = None) -> int:
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

        if count - last_commit >= SILVER_BATCH_SIZE:
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


async def run(source: str | None = None) -> int:
    if source is None:
        snap = await parse_snapshots()
        api = await parse_api_responses()
        return snap + api
    elif source in HTML_PARSERS:
        return await parse_snapshots(source)
    elif source in API_PARSERS:
        return await parse_api_responses(source)
    else:
        logger.warning("Unknown source %r — no parser registered", source)
        return 0
