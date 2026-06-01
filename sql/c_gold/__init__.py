"""Gold layer: dimensional product catalog and price facts. Schema is registered first."""

from sql.c_gold import schema  # noqa: F401  -- must import first (DDL listener)
from sql.c_gold.dim_product import DimProduct
from sql.c_gold.fact_price import FactPrice

__all__ = ["DimProduct", "FactPrice"]
