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


def shape_article_row(article: dict) -> dict:
    return {
        "source": article["source"],
        "source_url": article["source_url"],
        "title": article.get("title"),
        "subtitle": article.get("subtitle"),
        "body": article.get("body"),
        "author": article.get("author"),
        "published_at": _parse_date(article.get("published_at")),
        "section": article.get("section"),
        "image_url": article.get("image_url"),
        "word_count": _count_words(article.get("body")),
        "keywords": _extract_keywords(article.get("title")),
    }


def collect_image_urls(article: dict) -> list[str]:
    raw = article.get("image_urls")
    urls: list[str] = []
    if raw:
        try:
            urls = json.loads(raw) if isinstance(raw, str) else list(raw)
        except (json.JSONDecodeError, TypeError):
            urls = []
    if not urls and article.get("image_url"):
        urls = [article["image_url"]]
    return urls


async def flush_articles_chunk(articles: list[dict]) -> int:
    if not articles:
        return 0

    article_rows = [shape_article_row(a) for a in articles]
    await db.bulk_insert("silver.articles", article_rows)

    sources = [a["source"] for a in articles]
    source_urls = [a["source_url"] for a in articles]
    id_rows = await db.execute(
        """
        SELECT a.id, a.source, a.source_url
        FROM silver.articles a
        JOIN unnest(%s::text[], %s::text[]) AS t(source, source_url)
          ON a.source = t.source AND a.source_url = t.source_url
        """,
        [sources, source_urls],
    )
    key_to_id = {(r["source"], r["source_url"]): r["id"] for r in id_rows}

    image_rows: list[dict] = []
    for article in articles:
        silver_id = key_to_id.get((article["source"], article["source_url"]))
        if silver_id is None:
            continue
        hero_url = article.get("image_url")
        for idx, img_url in enumerate(collect_image_urls(article)):
            image_rows.append({
                "silver_article_id": silver_id,
                "image_url": img_url,
                "image_role": "hero" if idx == 0 and img_url == hero_url else "body",
                "ordinal": idx,
            })

    await db.bulk_insert("silver.article_images", image_rows)
    return len(articles)
