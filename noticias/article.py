from datetime import datetime, timezone

from dateutil import parser as dateparser
from psycopg import AsyncConnection
from pydantic import BaseModel, ConfigDict, Field, field_validator

from the_scraper import db

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
        return len(articles)
