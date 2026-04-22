import json

from dateutil import parser as dateparser

from the_scraper import db


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
