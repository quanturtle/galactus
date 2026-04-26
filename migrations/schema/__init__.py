"""SQLAlchemy metadata for Alembic autogenerate.

Runtime code uses raw psycopg and never touches these Table objects; they exist
only so Alembic can diff this declaration against the live database.

Primary keys use SERIAL / BIGSERIAL — matching the existing tables and the
SQLAlchemy default for `autoincrement=True`.
"""

from sqlalchemy import MetaData

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
}

metadata = MetaData(naming_convention=NAMING_CONVENTION)

from . import (  # noqa: E402, F401  — import for side-effect (table registration)
    bronze_snapshots,
    bronze_api_responses,
    silver_canonical_products,
    silver_products,
    silver_canonical_product_members,
    silver_product_images,
    silver_price_history,
    silver_articles,
    silver_article_tags,
    silver_article_entities,
    silver_article_images,
    gold_products,
)

__all__ = ["metadata"]
