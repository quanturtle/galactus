"""Parse raw bronze data and insert into silver.articles.

Reads from bronze.snapshots and bronze.api_responses where parsed_at IS NULL,
runs per-source parsers, enriches (dates, keywords, word count), and upserts
into silver.articles + silver.article_images.  Marks bronze rows as parsed.
"""

import json
import logging

from dateutil import parser as dateparser
from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from the_scraper.html_cleaner import decompress

from noticias.db.bronze import ApiResponse, Snapshot
from noticias.db.silver import Article as SilverArticle, ArticleImage
from noticias.parsers import API_PARSERS, HTML_PARSERS, parse_api_response, parse_snapshot

logger = logging.getLogger(__name__)

SNAPSHOT_SOURCES = set(HTML_PARSERS.keys())
API_SOURCES = set(API_PARSERS.keys())

SILVER_BATCH_SIZE = 1000

SPANISH_STOPWORDS = {
    "de", "la", "el", "en", "y", "a", "los", "las", "del", "un", "una",
    "que", "es", "se", "por", "con", "para", "su", "al", "lo", "como",
    "más", "pero", "sus", "le", "ya", "o", "fue", "este", "ha", "si",
    "no", "son", "entre", "cuando", "muy", "sin", "sobre", "ser", "también",
    "me", "hasta", "hay", "donde", "quien", "desde", "todo", "nos", "durante",
    "todos", "uno", "les", "ni", "contra", "otros", "ese", "eso", "ante",
    "ellos", "e", "esto", "mí", "antes", "algunos", "qué", "unos", "yo",
    "otro", "otras", "otra", "él", "tanto", "esa", "estos", "mucho", "quienes",
    "nada", "muchos", "cual", "poco", "ella", "estar", "estas", "algunas",
    "algo", "nosotros", "mi", "mis", "tú", "te", "ti", "tu", "tus",
}


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


async def _upsert_article(session: AsyncSession, article_dict: dict, fetched_at) -> None:
    published = _parse_date(article_dict.get("published_at"))
    word_count = _count_words(article_dict.get("body"))
    keywords = _extract_keywords(article_dict.get("title"))

    upsert = (
        insert(SilverArticle)
        .values(
            source=article_dict["source"],
            source_url=article_dict["source_url"],
            title=article_dict.get("title"),
            subtitle=article_dict.get("subtitle"),
            body=article_dict.get("body"),
            author=article_dict.get("author"),
            published_at=published,
            section=article_dict.get("section"),
            image_url=article_dict.get("image_url"),
            word_count=word_count,
            keywords=keywords,
        )
        .on_conflict_do_update(
            constraint="uq_silver_source_url",
            set_={
                "title": article_dict.get("title"),
                "subtitle": article_dict.get("subtitle"),
                "body": article_dict.get("body"),
                "author": article_dict.get("author"),
                "published_at": published,
                "section": article_dict.get("section"),
                "image_url": article_dict.get("image_url"),
                "word_count": word_count,
                "keywords": keywords,
                "processed_at": text("now()"),
            },
        )
        .returning(SilverArticle.id)
    )
    result = await session.execute(upsert)
    silver_id = result.scalar_one()

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
        img_upsert = (
            insert(ArticleImage)
            .values(
                silver_article_id=silver_id,
                image_url=img_url,
                image_role=role,
                ordinal=idx,
            )
            .on_conflict_do_nothing(constraint="uq_silver_article_image")
        )
        await session.execute(img_upsert)


async def parse_snapshots(session: AsyncSession, source: str | None = None) -> int:
    stmt = (
        select(Snapshot)
        .where(Snapshot.parsed_at.is_(None))
        .order_by(Snapshot.id)
    )
    if source:
        stmt = stmt.where(Snapshot.source == source)

    result = await session.execute(stmt)
    rows = result.scalars().all()

    if not rows:
        logger.info("No unparsed snapshots found")
        return 0

    logger.info("Processing %d unparsed snapshots", len(rows))

    count = 0
    last_commit = 0
    skipped = 0
    parsed_ids: list[int] = []

    for row in rows:
        html = decompress(bytes(row.html_blob))
        try:
            article_dict = parse_snapshot(row.source, html, row.url)
        except Exception:
            logger.exception("Failed to parse snapshot %d (%s)", row.id, row.url)
            parsed_ids.append(row.id)
            skipped += 1
            continue

        if article_dict:
            await _upsert_article(session, article_dict, row.fetched_at)
            count += 1
        else:
            skipped += 1

        parsed_ids.append(row.id)

        if count - last_commit >= SILVER_BATCH_SIZE:
            await session.execute(
                update(Snapshot)
                .where(Snapshot.id.in_(parsed_ids))
                .values(parsed_at=text("now()"))
            )
            await session.commit()
            logger.info("Snapshots: parsed %d, inserted %d into silver", len(parsed_ids), count)
            parsed_ids = []
            last_commit = count

    if parsed_ids:
        await session.execute(
            update(Snapshot)
            .where(Snapshot.id.in_(parsed_ids))
            .values(parsed_at=text("now()"))
        )
        await session.commit()

    logger.info("Snapshots: done — %d silver articles from %d snapshots, %d skipped", count, len(rows), skipped)
    return count


async def parse_api_responses(session: AsyncSession, source: str | None = None) -> int:
    stmt = (
        select(ApiResponse)
        .where(ApiResponse.parsed_at.is_(None))
        .order_by(ApiResponse.id)
    )
    if source:
        stmt = stmt.where(ApiResponse.source == source)

    result = await session.execute(stmt)
    rows = result.scalars().all()

    if not rows:
        logger.info("No unparsed API responses found")
        return 0

    logger.info("Processing %d unparsed API responses", len(rows))

    count = 0
    last_commit = 0
    skipped = 0
    parsed_ids: list[int] = []

    for row in rows:
        response_text = decompress(bytes(row.response_blob))
        try:
            articles = parse_api_response(row.source, response_text)
        except Exception:
            logger.exception("Failed to parse api_response %d (%s)", row.id, row.endpoint)
            parsed_ids.append(row.id)
            skipped += 1
            continue

        if not articles:
            skipped += 1
        else:
            for article_dict in articles:
                await _upsert_article(session, article_dict, row.fetched_at)
                count += 1

        parsed_ids.append(row.id)

        if count - last_commit >= SILVER_BATCH_SIZE:
            await session.execute(
                update(ApiResponse)
                .where(ApiResponse.id.in_(parsed_ids))
                .values(parsed_at=text("now()"))
            )
            await session.commit()
            logger.info("API responses: parsed %d, inserted %d into silver", len(parsed_ids), count)
            parsed_ids = []
            last_commit = count

    if parsed_ids:
        await session.execute(
            update(ApiResponse)
            .where(ApiResponse.id.in_(parsed_ids))
            .values(parsed_at=text("now()"))
        )
        await session.commit()

    logger.info("API responses: done — %d silver articles from %d responses, %d skipped", count, len(rows), skipped)
    return count


async def run(session: AsyncSession, source: str | None = None) -> int:
    if source is None:
        snap = await parse_snapshots(session)
        api = await parse_api_responses(session)
        return snap + api
    elif source in SNAPSHOT_SOURCES:
        return await parse_snapshots(session, source)
    elif source in API_SOURCES:
        return await parse_api_responses(session, source)
    else:
        logger.warning("Unknown source %r — no parser registered", source)
        return 0
