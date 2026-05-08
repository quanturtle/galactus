from datetime import datetime
from decimal import Decimal

from sqlalchemy import Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY
from sqlmodel import Column, Field, SQLModel

from sql.b_silver.schema import SCHEMA


class Product(SQLModel, table=True):
    """Silver entity: one supermarket product offering at a point in time."""

    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("source", "source_url", name="uq_products_source_url"),
        {"schema": SCHEMA},
    )

    id: int | None = Field(default=None, primary_key=True)
    source: str = Field(index=True)
    source_url: str
    sku: str | None = None
    name: str
    brand: str | None = None
    price: Decimal | None = Field(default=None, sa_column=Column(Numeric(12, 2)))
    currency: str | None = None
    unit: str | None = None
    in_stock: bool | None = None
    observed_at: datetime | None = Field(default=None, index=True)
    image_urls: list[str] = Field(
        default_factory=list,
        sa_column=Column(ARRAY(String), nullable=False, server_default="{}"),
    )
