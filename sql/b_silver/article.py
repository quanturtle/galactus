from datetime import datetime

from sqlalchemy import String, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from sql.b_silver.schema import SCHEMA
from sql.base import Base


class Article(Base):
    """Silver entity: a single news article parsed from one bronze snapshot.

    One row per (article, bronze sighting) — the same source_url appears once
    per snapshot that mentioned it. Dedup across sightings is the gold layer's job.
    """

    __tablename__ = "articles"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(primary_key=True)
    # the bronze row this entity was parsed from; (source, bronze_id) is its provenance key
    bronze_id: Mapped[int] = mapped_column(index=True)
    source: Mapped[str] = mapped_column(index=True)
    source_url: Mapped[str]
    title: Mapped[str]
    body_html: Mapped[str | None] = mapped_column(default=None)
    body_text: Mapped[str | None] = mapped_column(default=None)
    authors: Mapped[list[str]] = mapped_column(ARRAY(String), server_default="{}")
    published_at: Mapped[datetime | None] = mapped_column(index=True, default=None)
    section: Mapped[str | None] = mapped_column(default=None)
    tags: Mapped[list[str]] = mapped_column(ARRAY(String), server_default="{}")
    image_urls: Mapped[list[str]] = mapped_column(ARRAY(String), server_default="{}")
    # the bronze snapshot's created_at, stamped at parse time
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    def __init__(self, **kw) -> None:
        kw.setdefault("authors", [])
        kw.setdefault("tags", [])
        kw.setdefault("image_urls", [])
        super().__init__(**kw)
