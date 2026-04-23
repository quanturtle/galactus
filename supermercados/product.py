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
    images: list[str] = Field(default_factory=list, exclude=True)

    @classmethod
    async def persist_many(
        cls,
        products: list["Product"],
        *,
        conn: AsyncConnection | None = None,
    ) -> list[tuple[int, str, str]]:
        if not products:
            return []

        inserted = await db.bulk_insert(
            "silver.products",
            [p.model_dump() for p in products],
            conn=conn,
            conflict_columns=("source", "url"),
            update_columns=SILVER_PRODUCT_UPDATE_COLUMNS,
            returning=("id", "source", "url"),
        )

        id_map = {(r["source"], r["url"]): r["id"] for r in inserted}
        image_rows: list[dict] = []
        for product in products:
            product_id = id_map.get((product.source, product.url))
            if product_id is None:
                continue
            for ordinal, url in enumerate(product.images):
                image_rows.append({
                    "silver_product_id": product_id,
                    "image_url": url,
                    "image_role": "hero" if ordinal == 0 else "body",
                    "ordinal": ordinal,
                    "download_status": "pending",
                })

        if image_rows:
            await db.bulk_insert(
                "silver.product_images",
                image_rows,
                conn=conn,
                conflict_columns=("silver_product_id", "image_url"),
            )

        return [(r["id"], r["source"], r["url"]) for r in inserted]
