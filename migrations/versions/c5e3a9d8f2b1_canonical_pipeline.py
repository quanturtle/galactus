"""canonical pipeline

Revision ID: c5e3a9d8f2b1
Revises: 75be96bdd111
Create Date: 2026-04-25 12:00:00.000000

Introduces the silver canonicalization layer:

- ``silver.canonical_products`` — one row per logical product across sources.
- ``silver.canonical_product_members`` — junction table mapping silver products
  to their canonical product. ``silver.products`` itself is untouched.
- ``silver.price_history`` — append-only price log, populated by an
  ``AFTER INSERT`` / ``AFTER UPDATE OF price`` trigger on ``silver.products``.
- ``gold.products`` — analytical table aggregating canonical product +
  current price stats.
- Enables ``pg_trgm`` for fuzzy name matching (Step 2 of the canonical pipeline).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c5e3a9d8f2b1"
down_revision: Union[str, Sequence[str], None] = "75be96bdd111"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute("CREATE SCHEMA IF NOT EXISTS gold")

    op.create_table(
        "canonical_products",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("canonical_name", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"), nullable=False,
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"), nullable=False,
        ),
        sa.Column("llm_refined_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema="silver",
    )
    op.create_index(
        "idx_silver_canonical_products_name",
        "canonical_products", ["canonical_name"], schema="silver",
    )
    op.execute(
        "CREATE INDEX idx_silver_canonical_products_name_trgm "
        "ON silver.canonical_products USING gin (canonical_name gin_trgm_ops)"
    )

    op.create_table(
        "canonical_product_members",
        sa.Column("silver_product_id", sa.Integer(), nullable=False),
        sa.Column("canonical_product_id", sa.BigInteger(), nullable=False),
        sa.Column("match_method", sa.Text(), nullable=False),
        sa.Column(
            "matched_at", sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"), nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["silver_product_id"], ["silver.products.id"],
            name="fk_canonical_members_product", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["canonical_product_id"], ["silver.canonical_products.id"],
            name="fk_canonical_members_canonical", ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("silver_product_id"),
        schema="silver",
    )
    op.create_index(
        "idx_canonical_members_canonical",
        "canonical_product_members", ["canonical_product_id"], schema="silver",
    )

    op.create_table(
        "price_history",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("silver_product_id", sa.Integer(), nullable=False),
        sa.Column("price", sa.Integer(), nullable=False),
        sa.Column(
            "observed_at", sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"), nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["silver_product_id"], ["silver.products.id"],
            name="fk_silver_price_history_product",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="silver",
    )
    op.create_index(
        "idx_silver_price_history_product_time",
        "price_history",
        ["silver_product_id", sa.text("observed_at DESC")],
        schema="silver",
    )
    op.execute(
        "CREATE INDEX idx_silver_price_history_observed_brin "
        "ON silver.price_history USING brin (observed_at)"
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION silver.record_price_change()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.price IS NOT NULL THEN
                INSERT INTO silver.price_history (silver_product_id, price)
                VALUES (NEW.id, NEW.price);
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_silver_products_price_insert
        AFTER INSERT ON silver.products
        FOR EACH ROW
        WHEN (NEW.price IS NOT NULL)
        EXECUTE FUNCTION silver.record_price_change();
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_silver_products_price_update
        AFTER UPDATE OF price ON silver.products
        FOR EACH ROW
        WHEN (OLD.price IS DISTINCT FROM NEW.price)
        EXECUTE FUNCTION silver.record_price_change();
        """
    )

    op.create_table(
        "products",
        sa.Column("canonical_product_id", sa.BigInteger(), nullable=False),
        sa.Column("canonical_name", sa.Text(), nullable=False),
        sa.Column(
            "source_count", sa.Integer(), nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("current_min_price", sa.Integer(), nullable=True),
        sa.Column("current_max_price", sa.Integer(), nullable=True),
        sa.Column("current_avg_price", sa.Numeric(), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "price_changes_30d", sa.Integer(), nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"), nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["canonical_product_id"], ["silver.canonical_products.id"],
            name="fk_gold_products_canonical",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("canonical_product_id"),
        schema="gold",
    )


def downgrade() -> None:
    op.drop_table("products", schema="gold")

    op.execute("DROP TRIGGER IF EXISTS trg_silver_products_price_update ON silver.products")
    op.execute("DROP TRIGGER IF EXISTS trg_silver_products_price_insert ON silver.products")
    op.execute("DROP FUNCTION IF EXISTS silver.record_price_change()")

    op.drop_table("price_history", schema="silver")

    op.drop_index(
        "idx_canonical_members_canonical",
        table_name="canonical_product_members", schema="silver",
    )
    op.drop_table("canonical_product_members", schema="silver")

    op.execute("DROP INDEX IF EXISTS silver.idx_silver_canonical_products_name_trgm")
    op.drop_index(
        "idx_silver_canonical_products_name",
        table_name="canonical_products", schema="silver",
    )
    op.drop_table("canonical_products", schema="silver")
