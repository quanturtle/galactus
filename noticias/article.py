from datetime import datetime, timezone

from dateutil import parser as dateparser
from psycopg import AsyncConnection
from pydantic import BaseModel, ConfigDict, Field, field_validator

from galactus import db

SILVER_ARTICLE_UPDATE_COLUMNS = (
    "title", "subtitle", "body", "author", "published_at",
    "section", "image_url", "updated_at",
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
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    images: list[str] = Field(default_factory=list, exclude=True)

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
    ) -> list[tuple[int, str, str]]:
        if not articles:
            return []

        inserted = await db.bulk_insert(
            "silver.articles",
            [a.model_dump() for a in articles],
            conn=conn,
            conflict_columns=("source", "source_url"),
            update_columns=SILVER_ARTICLE_UPDATE_COLUMNS,
            returning=("id", "source", "source_url"),
        )

        id_map = {(r["source"], r["source_url"]): r["id"] for r in inserted}
        image_rows: list[dict] = []
        for article in articles:
            article_id = id_map.get((article.source, article.source_url))
            if article_id is None:
                continue
            for ordinal, url in enumerate(article.images):
                image_rows.append({
                    "silver_article_id": article_id,
                    "image_url": url,
                    "image_role": "hero" if ordinal == 0 else "body",
                    "ordinal": ordinal,
                    "download_status": "pending",
                })

        if image_rows:
            await db.bulk_insert(
                "silver.article_images",
                image_rows,
                conn=conn,
                conflict_columns=("silver_article_id", "image_url"),
            )

        return [(r["id"], r["source"], r["source_url"]) for r in inserted]
