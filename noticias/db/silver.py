from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, Index, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class SilverBase(DeclarativeBase):
    pass


class Article(SilverBase):
    __tablename__ = "articles"
    __table_args__ = (
        UniqueConstraint("source", "source_url", name="uq_silver_source_url"),
        Index("idx_silver_source", "source"),
        Index("idx_silver_published", "published_at"),
        Index("idx_silver_section", "section"),
        {"schema": "silver"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    bronze_id: Mapped[int | None] = mapped_column(BigInteger)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str | None] = mapped_column(Text)
    subtitle: Mapped[str | None] = mapped_column(Text)
    body: Mapped[str | None] = mapped_column(Text)
    author: Mapped[str | None] = mapped_column(String(255))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    section: Mapped[str | None] = mapped_column(String(100))
    image_url: Mapped[str | None] = mapped_column(Text)
    word_count: Mapped[int | None] = mapped_column(Integer)
    keywords: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ArticleTag(SilverBase):
    __tablename__ = "article_tags"
    __table_args__ = (
        UniqueConstraint("silver_article_id", name="uq_silver_article_tags"),
        Index("idx_silver_tags_article", "silver_article_id"),
        {"schema": "silver"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    silver_article_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tags: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ArticleEntity(SilverBase):
    __tablename__ = "article_entities"
    __table_args__ = (
        UniqueConstraint(
            "silver_article_id", "entity_name", "method",
            name="uq_silver_article_entity",
        ),
        Index("idx_silver_entities_article", "silver_article_id"),
        Index("idx_silver_entities_name", "normalized_name"),
        Index("idx_silver_entities_method", "method"),
        {"schema": "silver"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    silver_article_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    entity_name: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(String(20), nullable=False, server_default="PER")
    normalized_name: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float | None] = mapped_column(Float)
    mention_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    method: Mapped[str] = mapped_column(String(20), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ArticleImage(SilverBase):
    __tablename__ = "article_images"
    __table_args__ = (
        UniqueConstraint("silver_article_id", "image_url", name="uq_silver_article_image"),
        Index("idx_silver_images_article", "silver_article_id"),
        Index("idx_silver_images_status", "download_status"),
        Index("idx_silver_images_hash", "content_hash"),
        {"schema": "silver"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    silver_article_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    image_url: Mapped[str] = mapped_column(Text, nullable=False)
    image_role: Mapped[str] = mapped_column(String(20), nullable=False, server_default="hero")
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    s3_bucket: Mapped[str | None] = mapped_column(String(100))
    s3_key: Mapped[str | None] = mapped_column(Text)
    content_type: Mapped[str | None] = mapped_column(String(50))
    file_size_bytes: Mapped[int | None] = mapped_column(Integer)
    width: Mapped[int | None] = mapped_column(Integer)
    height: Mapped[int | None] = mapped_column(Integer)
    content_hash: Mapped[str | None] = mapped_column(String(64))
    download_status: Mapped[str] = mapped_column(String(20), nullable=False, server_default="pending")
    download_error: Mapped[str | None] = mapped_column(Text)
    downloaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
