from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Table,
    Text,
    text,
)

from . import metadata

Table(
    "canonical_products",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("canonical_name", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    Column("llm_refined_at", DateTime(timezone=True), nullable=True),
    Index("idx_silver_canonical_products_name", "canonical_name"),
    Index(
        "idx_silver_canonical_products_name_trgm",
        "canonical_name",
        postgresql_using="gin",
        postgresql_ops={"canonical_name": "gin_trgm_ops"},
    ),
    schema="silver",
)
