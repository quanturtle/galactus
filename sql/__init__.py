"""Persistence models. Importing this package registers every table and
schema-creation hook on Base.metadata so Alembic autogenerate sees them."""

from sql import a_bronze, b_silver, c_gold  # noqa: F401
from sql.a_bronze import ApiSnapshot, FailedSnapshot, HtmlSnapshot, Snapshot
from sql.b_silver import Article, Product
from sql.base import Base

__all__ = [
    "ApiSnapshot",
    "Article",
    "Base",
    "FailedSnapshot",
    "HtmlSnapshot",
    "Product",
    "Snapshot",
]
