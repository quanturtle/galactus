"""Silver layer: articles, products. Schema is registered first."""

from sql.b_silver import schema  # noqa: F401  -- must import first (DDL listener)
from sql.b_silver.article import Article
from sql.b_silver.product import Product

__all__ = ["Article", "Product"]
