from datetime import date, datetime

from sqlalchemy import BigInteger, Date, DateTime, Index, JSON, LargeBinary, Text, UniqueConstraint, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BronzeBase(DeclarativeBase):
    pass


class Article(BronzeBase):
    __tablename__ = "articles"
    __table_args__ = (
        Index("idx_bronze_source_url", "source", "source_url"),
        Index("idx_bronze_scraped_at", "scraped_at"),
        {"schema": "bronze"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str | None] = mapped_column(Text)
    subtitle: Mapped[str | None] = mapped_column(Text)
    body: Mapped[str | None] = mapped_column(Text)
    author: Mapped[str | None] = mapped_column(Text)
    published_at: Mapped[str | None] = mapped_column(Text)
    section: Mapped[str | None] = mapped_column(Text)
    image_url: Mapped[str | None] = mapped_column(Text)
    image_urls: Mapped[str | None] = mapped_column(Text)
    raw_data: Mapped[str | None] = mapped_column(Text)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Snapshot(BronzeBase):
    __tablename__ = "snapshots"
    __table_args__ = (
        UniqueConstraint("source", "url", "fetch_date", name="uq_snapshots_source_url_date"),
        Index(
            "idx_snapshots_unparsed", "source",
            postgresql_where=text("parsed_at IS NULL"),
        ),
        {"schema": "bronze"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    html_blob: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    fetch_date: Mapped[date] = mapped_column(
        Date, nullable=False, server_default=text("CURRENT_DATE"),
    )
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    parsed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ApiResponse(BronzeBase):
    __tablename__ = "api_responses"
    __table_args__ = (
        UniqueConstraint("source", "endpoint", "fetch_date", name="uq_api_responses_source_endpoint_date"),
        Index(
            "idx_api_responses_unparsed", "source",
            postgresql_where=text("parsed_at IS NULL"),
        ),
        {"schema": "bronze"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    endpoint: Mapped[str] = mapped_column(Text, nullable=False)
    page_params: Mapped[dict | None] = mapped_column(JSON)
    response_blob: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    fetch_date: Mapped[date] = mapped_column(
        Date, nullable=False, server_default=text("CURRENT_DATE"),
    )
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    parsed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
