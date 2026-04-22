"""SQLAlchemy metadata for Alembic autogenerate.

Runtime code uses raw psycopg and never touches these Table objects; they exist
only so Alembic can diff this declaration against the live database.

Primary keys use SERIAL / BIGSERIAL — matching the existing tables and the
SQLAlchemy default for `autoincrement=True`.
"""

from sqlalchemy import MetaData

metadata = MetaData()

from . import (  # noqa: E402, F401  — import for side-effect (table registration)
    bronze_snapshots,
    bronze_api_responses,
    silver_products,
    silver_articles,
    silver_article_tags,
    silver_article_entities,
    silver_article_images,
)

__all__ = ["metadata"]
