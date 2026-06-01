from datetime import datetime
from decimal import Decimal

from sqlalchemy import ForeignKey, Numeric, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from sql.base import Base
from sql.c_gold.schema import SCHEMA


class FactPrice(Base):
    """Gold fact: a price-change event for one product.

    One row per change-point — a product's price is recorded only when it
    differs from the previous observation, so consecutive identical prices
    collapse into a single row. observed_at is the snapshot time at which the
    new price first appeared; a flat segment between two change-points is
    reconstructed downstream. (product_key, observed_at) is the idempotency key.
    """

    __tablename__ = "fact_price"
    __table_args__ = (
        UniqueConstraint("product_key", "observed_at", name="uq_fact_price_product_observed"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    product_key: Mapped[int] = mapped_column(
        ForeignKey(f"{SCHEMA}.dim_product.product_key"), index=True
    )
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    currency: Mapped[str] = mapped_column(default="PYG", server_default="PYG")
    observed_at: Mapped[datetime]
    # the silver row's bronze origin, carried through for provenance
    bronze_id: Mapped[int]
