"""Persistence models. Importing this package registers every table and
schema-creation hook on SQLModel.metadata so Alembic autogenerate sees them."""

from sql import a_bronze, b_silver, c_gold  # noqa: F401
from sql.a_bronze import ApiSnapshot, HtmlSnapshot, RawRecord
from sql.b_silver import Article, ParsedRecord, Product

__all__ = [
    "ApiSnapshot",
    "Article",
    "HtmlSnapshot",
    "ParsedRecord",
    "Product",
    "RawRecord",
]
