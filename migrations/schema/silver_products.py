from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    Table,
    Text,
    UniqueConstraint,
    text,
)

from . import metadata

Table(
    "products",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source", Text, nullable=False),
    Column("url", Text, nullable=False),
    Column("name", Text, nullable=False),
    Column("description", Text),
    Column("price", Integer),
    Column("sku", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    UniqueConstraint("source", "url"),
    Index("idx_silver_products_source", "source"),
    Index("idx_silver_products_sku", "sku"),
    schema="silver",
)
