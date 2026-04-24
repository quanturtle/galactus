"""drop image statuses, add bronze images_processed_at

Revision ID: 75be96bdd111
Revises: a3f5c1e0b829
Create Date: 2026-04-24 20:06:50.657490

Switches the image pipeline to inventory-only child rows:

* drops ``download_status`` + ``download_error`` columns and the partial
  status index from ``silver.article_images`` and ``silver.product_images``.
  A row in those tables now exists ⟺ the bytes are in S3.
* adds ``images_processed_at`` to ``bronze.snapshots`` and
  ``bronze.api_responses`` to track whether the downloader has already tried
  this row's images. No retry on per-URL failure — set and forget.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "75be96bdd111"
down_revision: Union[str, Sequence[str], None] = "a3f5c1e0b829"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "api_responses",
        sa.Column("images_processed_at", sa.DateTime(timezone=True), nullable=True),
        schema="bronze",
    )
    op.create_index(
        "idx_api_responses_images_pending",
        "api_responses",
        ["source"],
        unique=False,
        schema="bronze",
        postgresql_where=sa.text("images_processed_at IS NULL"),
    )
    op.add_column(
        "snapshots",
        sa.Column("images_processed_at", sa.DateTime(timezone=True), nullable=True),
        schema="bronze",
    )
    op.create_index(
        "idx_snapshots_images_pending",
        "snapshots",
        ["source"],
        unique=False,
        schema="bronze",
        postgresql_where=sa.text("images_processed_at IS NULL"),
    )
    op.drop_index("idx_silver_images_status", table_name="article_images", schema="silver")
    op.drop_column("article_images", "download_error", schema="silver")
    op.drop_column("article_images", "download_status", schema="silver")
    op.drop_index(
        "idx_silver_product_images_status", table_name="product_images", schema="silver"
    )
    op.drop_column("product_images", "download_error", schema="silver")
    op.drop_column("product_images", "download_status", schema="silver")


def downgrade() -> None:
    op.add_column(
        "product_images",
        sa.Column(
            "download_status",
            sa.VARCHAR(length=20),
            server_default=sa.text("'pending'::character varying"),
            autoincrement=False,
            nullable=False,
        ),
        schema="silver",
    )
    op.add_column(
        "product_images",
        sa.Column("download_error", sa.TEXT(), autoincrement=False, nullable=True),
        schema="silver",
    )
    op.create_index(
        "idx_silver_product_images_status",
        "product_images",
        ["download_status"],
        unique=False,
        schema="silver",
    )
    op.add_column(
        "article_images",
        sa.Column(
            "download_status",
            sa.VARCHAR(length=20),
            server_default=sa.text("'pending'::character varying"),
            autoincrement=False,
            nullable=False,
        ),
        schema="silver",
    )
    op.add_column(
        "article_images",
        sa.Column("download_error", sa.TEXT(), autoincrement=False, nullable=True),
        schema="silver",
    )
    op.create_index(
        "idx_silver_images_status",
        "article_images",
        ["download_status"],
        unique=False,
        schema="silver",
    )
    op.drop_index(
        "idx_snapshots_images_pending",
        table_name="snapshots",
        schema="bronze",
        postgresql_where=sa.text("images_processed_at IS NULL"),
    )
    op.drop_column("snapshots", "images_processed_at", schema="bronze")
    op.drop_index(
        "idx_api_responses_images_pending",
        table_name="api_responses",
        schema="bronze",
        postgresql_where=sa.text("images_processed_at IS NULL"),
    )
    op.drop_column("api_responses", "images_processed_at", schema="bronze")
