from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY

from . import metadata

Table(
    "articles",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("bronze_id", BigInteger),
    Column("source", String(50), nullable=False),
    Column("source_url", Text, nullable=False),
    Column("title", Text),
    Column("subtitle", Text),
    Column("body", Text),
    Column("author", String(255)),
    Column("published_at", DateTime(timezone=True)),
    Column("section", String(100)),
    Column("image_url", Text),
    Column("word_count", Integer),
    Column("keywords", ARRAY(Text)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    UniqueConstraint("source", "source_url", name="uq_silver_source_url"),
    Index("idx_silver_source", "source"),
    Index("idx_silver_published", "published_at"),
    Index("idx_silver_section", "section"),
    schema="silver",
)
