"""baseline schema

Revision ID: 8386d58dc45a
Revises:
Create Date: 2026-04-22 18:22:12.939121

Creates the ``bronze`` and ``silver`` Postgres schemas and all seven tables
that previously lived in ``sql/00-schemas.sql`` through ``sql/07-*.sql``.

Existing databases (where these tables were already created by the docker init
scripts) should be marked current with ``alembic stamp 8386d58dc45a`` instead
of running this upgrade.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "8386d58dc45a"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    op.execute("CREATE SCHEMA IF NOT EXISTS silver")

    op.create_table(
        "snapshots",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("html_blob", sa.LargeBinary(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=True),
        sa.Column("fetch_date", sa.Date(), server_default=sa.text("CURRENT_DATE"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("parsed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source", "url", "fetch_date", name="uq_snapshots_source_url_date"),
        schema="bronze",
    )
    op.create_index(
        "idx_snapshots_unparsed",
        "snapshots",
        ["source"],
        schema="bronze",
        postgresql_where=sa.text("parsed_at IS NULL"),
    )

    op.create_table(
        "api_responses",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("endpoint", sa.Text(), nullable=False),
        sa.Column("page_params", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("response_blob", sa.LargeBinary(), nullable=False),
        sa.Column("fetch_date", sa.Date(), server_default=sa.text("CURRENT_DATE"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("parsed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source", "endpoint", "fetch_date", name="uq_api_responses_source_endpoint_date"
        ),
        schema="bronze",
    )
    op.create_index(
        "idx_api_responses_unparsed",
        "api_responses",
        ["source"],
        schema="bronze",
        postgresql_where=sa.text("parsed_at IS NULL"),
    )

    op.create_table(
        "products",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("price", sa.Integer(), nullable=True),
        sa.Column("sku", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source", "url"),
        schema="silver",
    )
    op.create_index("idx_silver_products_source", "products", ["source"], schema="silver")
    op.create_index("idx_silver_products_sku", "products", ["sku"], schema="silver")

    op.create_table(
        "articles",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("bronze_id", sa.BigInteger(), nullable=True),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("subtitle", sa.Text(), nullable=True),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("author", sa.String(length=255), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("section", sa.String(length=100), nullable=True),
        sa.Column("image_url", sa.Text(), nullable=True),
        sa.Column("word_count", sa.Integer(), nullable=True),
        sa.Column("keywords", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source", "source_url", name="uq_silver_source_url"),
        schema="silver",
    )
    op.create_index("idx_silver_source", "articles", ["source"], schema="silver")
    op.create_index("idx_silver_published", "articles", ["published_at"], schema="silver")
    op.create_index("idx_silver_section", "articles", ["section"], schema="silver")

    op.create_table(
        "article_tags",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("silver_article_id", sa.BigInteger(), nullable=False),
        sa.Column("tags", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("silver_article_id"),
        schema="silver",
    )
    op.create_index("idx_silver_tags_article", "article_tags", ["silver_article_id"], schema="silver")
    op.create_index(
        "idx_silver_tags_gin", "article_tags", ["tags"], schema="silver", postgresql_using="gin"
    )

    op.create_table(
        "article_entities",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("silver_article_id", sa.BigInteger(), nullable=False),
        sa.Column("entity_name", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.String(length=20), server_default=sa.text("'PER'"), nullable=False),
        sa.Column("normalized_name", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("mention_count", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("method", sa.String(length=20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("silver_article_id", "entity_name", "method"),
        schema="silver",
    )
    op.create_index("idx_silver_entities_article", "article_entities", ["silver_article_id"], schema="silver")
    op.create_index("idx_silver_entities_name", "article_entities", ["normalized_name"], schema="silver")
    op.create_index("idx_silver_entities_method", "article_entities", ["method"], schema="silver")

    op.create_table(
        "article_images",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("silver_article_id", sa.BigInteger(), nullable=False),
        sa.Column("image_url", sa.Text(), nullable=False),
        sa.Column("image_role", sa.String(length=20), server_default=sa.text("'hero'"), nullable=False),
        sa.Column("ordinal", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("s3_bucket", sa.String(length=100), nullable=True),
        sa.Column("s3_key", sa.Text(), nullable=True),
        sa.Column("content_type", sa.String(length=50), nullable=True),
        sa.Column("file_size_bytes", sa.Integer(), nullable=True),
        sa.Column("width", sa.Integer(), nullable=True),
        sa.Column("height", sa.Integer(), nullable=True),
        sa.Column("content_hash", sa.String(length=64), nullable=True),
        sa.Column("download_status", sa.String(length=20), server_default=sa.text("'pending'"), nullable=False),
        sa.Column("download_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("silver_article_id", "image_url", name="uq_silver_article_image"),
        schema="silver",
    )
    op.create_index("idx_silver_images_article", "article_images", ["silver_article_id"], schema="silver")
    op.create_index("idx_silver_images_status", "article_images", ["download_status"], schema="silver")
    op.create_index("idx_silver_images_hash", "article_images", ["content_hash"], schema="silver")


def downgrade() -> None:
    op.drop_table("article_images", schema="silver")
    op.drop_table("article_entities", schema="silver")
    op.drop_table("article_tags", schema="silver")
    op.drop_table("articles", schema="silver")
    op.drop_table("products", schema="silver")
    op.drop_table("api_responses", schema="bronze")
    op.drop_table("snapshots", schema="bronze")
    op.execute("DROP SCHEMA IF EXISTS silver")
    op.execute("DROP SCHEMA IF EXISTS bronze")
