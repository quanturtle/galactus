import json
from datetime import datetime, timezone

from dateutil import parser as dateparser
from psycopg import AsyncConnection
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from the_scraper import db

SILVER_ARTICLE_UPDATE_COLUMNS = (
    "title", "subtitle", "body", "author", "published_at",
    "section", "image_url", "processed_at",
)


class Article(BaseModel):
    """Silver-layer article. `model_dump()` yields a row for `silver.articles`."""

    model_config = ConfigDict(extra="ignore")

    source: str
    source_url: str
    title: str | None = None
    subtitle: str | None = None
    body: str | None = None
    author: str | None = None
    published_at: datetime | None = None
    section: str | None = None
    image_url: str | None = None
    processed_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    images: list[str] = Field(default_factory=list, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def _collect_images(cls, data):
        if not isinstance(data, dict):
            return data
        raw = data.get("image_urls")
        urls: list[str] = []
        if raw:
            try:
                urls = json.loads(raw) if isinstance(raw, str) else list(raw)
            except (json.JSONDecodeError, TypeError):
                urls = []
        if not urls and data.get("image_url"):
            urls = [data["image_url"]]
        return {**data, "images": urls}

    @field_validator("published_at", mode="before")
    @classmethod
    def _parse_published_at(cls, v):
        if not v:
            return None
        if isinstance(v, datetime):
            return v
        try:
            return dateparser.parse(v)
        except (ValueError, OverflowError, TypeError):
            return None

    @classmethod
    async def persist_many(
        cls,
        articles: list["Article"],
        *,
        conn: AsyncConnection | None = None,
    ) -> int:
        if not articles:
            return 0

        await db.bulk_insert(
            "silver.articles",
            [a.model_dump() for a in articles],
            conn=conn,
            conflict_columns=("source", "source_url"),
            update_columns=SILVER_ARTICLE_UPDATE_COLUMNS,
        )

        sources = [a.source for a in articles]
        source_urls = [a.source_url for a in articles]
        id_rows = await db.execute(
            """
            SELECT a.id, a.source, a.source_url
            FROM silver.articles a
            JOIN unnest(%s::text[], %s::text[]) AS t(source, source_url)
              ON a.source = t.source AND a.source_url = t.source_url
            """,
            [sources, source_urls],
            conn=conn,
        )
        key_to_id = {(r["source"], r["source_url"]): r["id"] for r in id_rows}

        image_rows: list[dict] = []
        for article in articles:
            silver_id = key_to_id.get((article.source, article.source_url))
            if silver_id is None:
                continue
            hero_url = article.image_url
            for idx, img_url in enumerate(article.images):
                image_rows.append({
                    "silver_article_id": silver_id,
                    "image_url": img_url,
                    "image_role": "hero" if idx == 0 and img_url == hero_url else "body",
                    "ordinal": idx,
                })

        await db.bulk_insert("silver.article_images", image_rows, conn=conn)
        return len(articles)
