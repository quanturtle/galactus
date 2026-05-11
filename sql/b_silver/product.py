from datetime import datetime
from decimal import Decimal

from sqlalchemy import Numeric, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from sql.b_silver.schema import SCHEMA
from sql.base import Base


class Product(Base):
    """Silver entity: one supermarket product offering at a point in time."""

    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("source", "source_url", name="uq_products_source_url"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(index=True)
    source_url: Mapped[str]
    sku: Mapped[str | None] = mapped_column(default=None)
    name: Mapped[str]
    brand: Mapped[str | None] = mapped_column(default=None)
    price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), default=None)
    currency: Mapped[str | None] = mapped_column(default=None)
    unit: Mapped[str | None] = mapped_column(default=None)
    in_stock: Mapped[bool | None] = mapped_column(default=None)
    image_urls: Mapped[list[str]] = mapped_column(ARRAY(String), server_default="{}")
    # bronze provenance: first / latest bronze.created_at seen for this (source, source_url)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now())

    def __init__(self, **kw) -> None:
        kw.setdefault("image_urls", [])
        super().__init__(**kw)
