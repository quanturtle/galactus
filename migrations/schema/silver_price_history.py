from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Table,
    text,
)

from . import metadata

Table(
    "price_history",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column(
        "silver_product_id",
        Integer,
        ForeignKey("silver.products.id", ondelete="CASCADE", name="fk_silver_price_history_product"),
        nullable=False,
    ),
    Column("price", Integer, nullable=False),
    Column("observed_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    Index("idx_silver_price_history_product_time", "silver_product_id", "observed_at"),
    Index(
        "idx_silver_price_history_observed_brin",
        "observed_at",
        postgresql_using="brin",
    ),
    schema="silver",
)
