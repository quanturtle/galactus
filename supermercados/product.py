from datetime import datetime

from pydantic import BaseModel, ConfigDict

from the_scraper import db


class Product(BaseModel):
    """Silver-layer product. `model_dump()` yields a row for `silver.products`."""

    model_config = ConfigDict(extra="ignore")

    source: str
    url: str
    name: str
    description: str | None = None
    price: int | None = None
    sku: str | None = None
    scraped_at: datetime

    @classmethod
    async def persist_many(cls, products: list["Product"]) -> int:
        if not products:
            return 0
        await db.bulk_insert("silver.products", [p.model_dump() for p in products])
        return len(products)
