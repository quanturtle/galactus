from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Table,
    Text,
    text,
)

from . import metadata

Table(
    "canonical_product_members",
    metadata,
    Column(
        "silver_product_id",
        Integer,
        ForeignKey(
            "silver.products.id",
            ondelete="CASCADE",
            name="fk_canonical_members_product",
        ),
        primary_key=True,
    ),
    Column(
        "canonical_product_id",
        BigInteger,
        ForeignKey(
            "silver.canonical_products.id",
            ondelete="CASCADE",
            name="fk_canonical_members_canonical",
        ),
        nullable=False,
    ),
    Column("match_method", Text, nullable=False),
    Column("matched_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    Index("idx_canonical_members_canonical", "canonical_product_id"),
    schema="silver",
)
