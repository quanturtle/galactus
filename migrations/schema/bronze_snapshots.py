from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Index,
    LargeBinary,
    Table,
    Text,
    UniqueConstraint,
    text,
)

from . import metadata

Table(
    "snapshots",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("source", Text, nullable=False),
    Column("url", Text, nullable=False),
    Column("html_blob", LargeBinary, nullable=False),
    Column("content_hash", Text),
    Column("fetch_date", Date, nullable=False, server_default=text("CURRENT_DATE")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    Column("parsed_at", DateTime(timezone=True)),
    Column("images_processed_at", DateTime(timezone=True)),
    UniqueConstraint("source", "url", "fetch_date", name="uq_snapshots_source_url_date"),
    Index("idx_snapshots_unparsed", "source", postgresql_where=text("parsed_at IS NULL")),
    Index(
        "idx_snapshots_images_pending",
        "source",
        postgresql_where=text("images_processed_at IS NULL"),
    ),
    schema="bronze",
)
