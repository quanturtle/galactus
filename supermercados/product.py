from datetime import datetime, timezone

from psycopg import AsyncConnection
from pydantic import BaseModel, ConfigDict, Field

from galactus import db

SILVER_PRODUCT_UPDATE_COLUMNS = (
    "name", "description", "price", "sku", "updated_at",
)


class Product(BaseModel):
    """Silver-layer product. `model_dump()` yields a row for `silver.products`."""

    model_config = ConfigDict(extra="ignore")

    source: str
    url: str
    name: str
    description: str | None = None
    price: int | None = None
    sku: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @classmethod
    async def persist_many(
        cls,
        products: list["Product"],
        *,
        conn: AsyncConnection | None = None,
    ) -> int:
        if not products:
            return 0
        await db.bulk_insert(
            "silver.products",
            [p.model_dump() for p in products],
            conn=conn,
            conflict_columns=("source", "url"),
            update_columns=SILVER_PRODUCT_UPDATE_COLUMNS,
        )
        return len(products)
