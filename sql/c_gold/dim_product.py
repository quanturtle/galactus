from datetime import datetime

from sqlalchemy import String, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from sql.base import Base
from sql.c_gold.schema import SCHEMA


class DimProduct(Base):
    """Gold dimension: one deduplicated product, keyed by (source, sku).

    Collapses the many silver sightings of the same offering into a single row
    carrying its latest known descriptive attributes and the window over which
    it has been seen. product_key is a surrogate the fact table references.
    Merging products that share a name but differ in sku is a later step.
    """

    __tablename__ = "dim_product"
    __table_args__ = (
        UniqueConstraint("source", "sku", name="uq_dim_product_source_sku"),
        {"schema": SCHEMA},
    )

    product_key: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(index=True)
    sku: Mapped[str]
    name: Mapped[str]
    brand: Mapped[str | None] = mapped_column(default=None)
    unit: Mapped[str | None] = mapped_column(default=None)
    currency: Mapped[str] = mapped_column(default="PYG", server_default="PYG")
    image_urls: Mapped[list[str]] = mapped_column(ARRAY(String), server_default="{}")
    first_seen_at: Mapped[datetime]
    last_seen_at: Mapped[datetime]

    def __init__(self, **kw) -> None:
        kw.setdefault("image_urls", [])
        super().__init__(**kw)
