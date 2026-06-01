import logging
from collections import defaultdict
from decimal import Decimal

from galactus.config import LoadConfig
from galactus.infra.db import Database
from sql.b_silver.product import Product
from sql.base import Base
from sql.c_gold.dim_product import DimProduct
from sql.c_gold.fact_price import FactPrice

logger = logging.getLogger(__name__)

DIM_UPDATE_COLUMNS = [
    "name",
    "brand",
    "unit",
    "currency",
    "image_urls",
    "first_seen_at",
    "last_seen_at",
]


class Builder:
    """Aggregates silver products into the gold star schema for one source.

    Collapses the many silver sightings of each (source, sku) into one
    dim_product row carrying its latest attributes and seen-window, then walks
    each product's price history in snapshot order and records a fact_price row
    only where the price changes. Both writes go through the idempotent
    Database.upsert, so the whole run is safe to repeat. Rows with a null sku
    can't be keyed and are skipped; they wait for the future name-merge step.
    """

    def __init__(self, config: LoadConfig) -> None:
        self.config = config
        self.source = config.source
        # populated in run(), inside the async with
        self.db: Database
        logger.info("Builder initialized (source=%s, builder=%s)", self.source, config.builder)

    def make_database(self) -> Database:
        return Database(database_url=self.config.database_url, pool_size=self.config.db_pool_size)

    def build_dim_products(self, products: list[Product]) -> list[DimProduct]:
        # group sightings by sku, then keep the newest as the canonical attributes
        by_sku: dict[str, list[Product]] = defaultdict(list)
        for product in products:
            if product.sku is not None:
                by_sku[product.sku].append(product)
        dims: list[DimProduct] = []
        for sku, sightings in by_sku.items():
            sightings.sort(key=lambda p: (p.created_at, p.id))
            latest = sightings[-1]
            dims.append(
                DimProduct(
                    source=latest.source,
                    sku=sku,
                    name=latest.name,
                    brand=latest.brand,
                    unit=latest.unit,
                    currency=latest.currency,
                    image_urls=latest.image_urls,
                    first_seen_at=sightings[0].created_at,
                    last_seen_at=latest.created_at,
                )
            )
        return dims

    async def upsert_chunked(
        self,
        records: list[Base],
        model: type[Base],
        index_elements: list[str],
        update_columns: list[str] | None = None,
        chunk_size: int = 100,
    ) -> None:
        """Upsert records in chunks so one INSERT stays under Postgres' 65535-param ceiling."""
        for start in range(0, len(records), chunk_size):
            await self.db.upsert(
                records[start : start + chunk_size],
                model=model,
                index_elements=index_elements,
                update_columns=update_columns,
            )
        return

    def build_price_changes(
        self, products: list[Product], product_key_by_sku: dict[str, int]
    ) -> list[FactPrice]:
        # group priced sightings by sku, then emit a row each time the price differs
        by_sku: dict[str, list[Product]] = defaultdict(list)
        for product in products:
            if product.sku is not None and product.price is not None:
                by_sku[product.sku].append(product)
        facts: list[FactPrice] = []
        for sku, sightings in by_sku.items():
            product_key = product_key_by_sku.get(sku)
            if product_key is None:
                continue
            sightings.sort(key=lambda p: (p.created_at, p.id))
            previous_price: Decimal | None = None
            for sighting in sightings:
                if sighting.price != previous_price:
                    facts.append(
                        FactPrice(
                            product_key=product_key,
                            price=sighting.price,
                            currency=sighting.currency,
                            observed_at=sighting.created_at,
                            bronze_id=sighting.bronze_id,
                        )
                    )
                    previous_price = sighting.price
        return facts

    async def run(self) -> None:
        """Lifecycle: open db; read silver; upsert the dim; key the facts; upsert them."""
        async with self.make_database() as db:
            self.db = db
            batch_size = self.config.batch_size

            # read silver and collapse it into the product dimension
            products = await self.db.fetch(Product, source=self.source)
            skipped = sum(1 for product in products if product.sku is None)
            dims = self.build_dim_products(products)
            await self.upsert_chunked(
                dims,
                model=DimProduct,
                index_elements=["source", "sku"],
                update_columns=DIM_UPDATE_COLUMNS,
                chunk_size=batch_size,
            )

            # resolve surrogate keys, then write the price change-points
            stored = await self.db.fetch(DimProduct, source=self.source)
            product_key_by_sku = {dim.sku: dim.product_key for dim in stored}
            facts = self.build_price_changes(products, product_key_by_sku)
            await self.upsert_chunked(
                facts,
                model=FactPrice,
                index_elements=["product_key", "observed_at"],
                chunk_size=batch_size,
            )
        logger.info(
            "load[%s]: complete (%s products, %s null-sku skipped, %s price change-points)",
            self.source,
            len(dims),
            skipped,
            len(facts),
        )
        return
