from datetime import datetime
from decimal import Decimal

from sqlalchemy import Numeric, String, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from sql.b_silver.schema import SCHEMA
from sql.base import Base


class Product(Base):
    """Silver entity: one supermarket product offering parsed from one bronze snapshot.

    One row per (offering, bronze sighting) — the same source_url appears once
    per snapshot that mentioned it. Dedup across sightings is the gold layer's job.
    """

    __tablename__ = "products"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(primary_key=True)
    # the bronze row this entity was parsed from; (source, bronze_id) is its provenance key
    bronze_id: Mapped[int] = mapped_column(index=True)
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
    # the bronze snapshot's created_at, stamped at parse time
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    def __init__(self, **kw) -> None:
        kw.setdefault("image_urls", [])
        super().__init__(**kw)
