"""Persistence models. Importing this package registers every table and
schema-creation hook on Base.metadata so Alembic autogenerate sees them."""

from sql import a_bronze, b_silver, c_gold  # noqa: F401
from sql.a_bronze import ApiSnapshot, FailedSnapshot, HtmlSnapshot, Snapshot
from sql.b_silver import Article, Product
from sql.base import Base
from sql.c_gold import DimProduct, FactPrice

__all__ = [
    "ApiSnapshot",
    "Article",
    "Base",
    "DimProduct",
    "FactPrice",
    "FailedSnapshot",
    "HtmlSnapshot",
    "Product",
    "Snapshot",
]
