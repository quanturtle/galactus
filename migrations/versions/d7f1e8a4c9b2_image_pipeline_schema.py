"""image pipeline schema

Revision ID: d7f1e8a4c9b2
Revises: b4d2e7f1a3c9
Create Date: 2026-04-23 07:00:00.000000

Aligns the schema with the image-pipeline work:

* drops ``bronze_id``, ``word_count``, ``keywords`` from ``silver.articles`` —
  unused since the bronze→silver transform stopped populating them.
* adds FK ``silver.article_images.silver_article_id`` → ``silver.articles.id``
  with ``ON DELETE CASCADE``.
* creates ``silver.product_images`` mirroring ``silver.article_images`` but
  keyed on ``silver.products.id`` (Integer, not BigInteger).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "d7f1e8a4c9b2"
down_revision: Union[str, Sequence[str], None] = "b4d2e7f1a3c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("articles", "bronze_id", schema="silver")
    op.drop_column("articles", "word_count", schema="silver")
    op.drop_column("articles", "keywords", schema="silver")

    op.create_foreign_key(
        "fk_silver_article_images_article",
        "article_images",
        "articles",
        ["silver_article_id"],
        ["id"],
        source_schema="silver",
        referent_schema="silver",
        ondelete="CASCADE",
    )

    op.create_table(
        "product_images",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("silver_product_id", sa.Integer(), nullable=False),
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
        sa.ForeignKeyConstraint(
            ["silver_product_id"],
            ["silver.products.id"],
            name="fk_silver_product_images_product",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("silver_product_id", "image_url", name="uq_silver_product_image"),
        schema="silver",
    )
    op.create_index("idx_silver_product_images_product", "product_images", ["silver_product_id"], schema="silver")
    op.create_index("idx_silver_product_images_status", "product_images", ["download_status"], schema="silver")
    op.create_index("idx_silver_product_images_hash", "product_images", ["content_hash"], schema="silver")


def downgrade() -> None:
    op.drop_index("idx_silver_product_images_hash", table_name="product_images", schema="silver")
    op.drop_index("idx_silver_product_images_status", table_name="product_images", schema="silver")
    op.drop_index("idx_silver_product_images_product", table_name="product_images", schema="silver")
    op.drop_table("product_images", schema="silver")

    op.drop_constraint(
        "fk_silver_article_images_article",
        "article_images",
        schema="silver",
        type_="foreignkey",
    )

    op.add_column(
        "articles",
        sa.Column("keywords", postgresql.ARRAY(sa.Text()), nullable=True),
        schema="silver",
    )
    op.add_column(
        "articles",
        sa.Column("word_count", sa.Integer(), nullable=True),
        schema="silver",
    )
    op.add_column(
        "articles",
        sa.Column("bronze_id", sa.BigInteger(), nullable=True),
        schema="silver",
    )
