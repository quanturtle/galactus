from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

SILVER_TABLE = "silver.products"


class Product(BaseModel):
    """Silver entity: a single supermarket product offering at a point in time.

    Persistence is handled by SilverRepo, not by this model.
    """

    source: str
    source_url: str
    sku: str | None = None
    name: str
    brand: str | None = None
    price: Decimal | None = None
    currency: str | None = None
    unit: str | None = None
    in_stock: bool | None = None
    observed_at: datetime | None = None
    image_urls: list[str] = Field(default_factory=list)
