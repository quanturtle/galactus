from datetime import datetime

from sqlalchemy import String, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY
from sqlmodel import Column, Field, SQLModel

from sql.b_silver.schema import SCHEMA


class Article(SQLModel, table=True):
    """Silver entity: a single news article."""

    __tablename__ = "articles"
    __table_args__ = (
        UniqueConstraint("source", "source_url", name="uq_articles_source_url"),
        {"schema": SCHEMA},
    )

    id: int | None = Field(default=None, primary_key=True)
    source: str = Field(index=True)
    source_url: str
    title: str
    body_html: str | None = None
    body_text: str | None = None
    authors: list[str] = Field(
        default_factory=list,
        sa_column=Column(ARRAY(String), nullable=False, server_default="{}"),
    )
    published_at: datetime | None = Field(default=None, index=True)
    section: str | None = None
    tags: list[str] = Field(
        default_factory=list,
        sa_column=Column(ARRAY(String), nullable=False, server_default="{}"),
    )
    image_urls: list[str] = Field(
        default_factory=list,
        sa_column=Column(ARRAY(String), nullable=False, server_default="{}"),
    )
