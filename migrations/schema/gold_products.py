from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    Table,
    Text,
    text,
)

from . import metadata

Table(
    "products",
    metadata,
    Column(
        "canonical_product_id",
        BigInteger,
        ForeignKey(
            "silver.canonical_products.id",
            ondelete="CASCADE",
            name="fk_gold_products_canonical",
        ),
        primary_key=True,
    ),
    Column("canonical_name", Text, nullable=False),
    Column("source_count", Integer, nullable=False, server_default=text("0")),
    Column("current_min_price", Integer),
    Column("current_max_price", Integer),
    Column("current_avg_price", Numeric),
    Column("last_seen_at", DateTime(timezone=True)),
    Column("price_changes_30d", Integer, nullable=False, server_default=text("0")),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    schema="gold",
)
