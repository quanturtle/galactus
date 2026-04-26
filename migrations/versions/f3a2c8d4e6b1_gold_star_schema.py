"""gold star schema

Revision ID: f3a2c8d4e6b1
Revises: c5e3a9d8f2b1
Create Date: 2026-04-26 12:00:00.000000

Replaces the ad-hoc ``gold.products`` aggregate with a star schema:

- ``gold.dim_dates`` — date dimension table seeded 2024-01-01 .. 2030-12-31.
- ``gold.dim_products`` — view, one row per canonical product.
- ``gold.fact_prices`` — view, one row per ``silver.price_history`` observation.

Also one-shot backfills ``silver.price_history`` for every priced silver
product that predates the price-change trigger (migration c5e3a9d8f2b1
was applied ~4 days after silver was first populated, leaving most
products without a left anchor for time-series charts). Backfill uses
``silver.products.created_at`` as the synthetic observation timestamp.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f3a2c8d4e6b1"
down_revision: Union[str, Sequence[str], None] = "c5e3a9d8f2b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS gold.products")

    op.execute(
        """
        INSERT INTO silver.price_history (silver_product_id, price, observed_at)
        SELECT p.id, p.price, p.created_at
        FROM silver.products p
        LEFT JOIN silver.price_history ph ON ph.silver_product_id = p.id
        WHERE p.price IS NOT NULL AND ph.id IS NULL
        """
    )

    op.create_table(
        "dim_dates",
        sa.Column("date_key", sa.Integer(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("year", sa.SmallInteger(), nullable=False),
        sa.Column("quarter", sa.SmallInteger(), nullable=False),
        sa.Column("month", sa.SmallInteger(), nullable=False),
        sa.Column("month_name", sa.Text(), nullable=False),
        sa.Column("day", sa.SmallInteger(), nullable=False),
        sa.Column("day_of_week", sa.SmallInteger(), nullable=False),
        sa.Column("day_name", sa.Text(), nullable=False),
        sa.Column("week_of_year", sa.SmallInteger(), nullable=False),
        sa.Column("is_weekend", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("date_key"),
        sa.UniqueConstraint("date", name="uq_dim_dates_date"),
        schema="gold",
    )

    op.execute(
        """
        INSERT INTO gold.dim_dates (
            date_key, date, year, quarter, month, month_name,
            day, day_of_week, day_name, week_of_year, is_weekend
        )
        SELECT
            (TO_CHAR(d, 'YYYYMMDD'))::int,
            d::date,
            EXTRACT(YEAR FROM d)::smallint,
            EXTRACT(QUARTER FROM d)::smallint,
            EXTRACT(MONTH FROM d)::smallint,
            TO_CHAR(d, 'FMMonth'),
            EXTRACT(DAY FROM d)::smallint,
            EXTRACT(DOW FROM d)::smallint,
            TO_CHAR(d, 'FMDay'),
            EXTRACT(WEEK FROM d)::smallint,
            EXTRACT(ISODOW FROM d) IN (6, 7)
        FROM generate_series('2024-01-01'::date, '2030-12-31'::date, '1 day') d
        ON CONFLICT (date_key) DO NOTHING
        """
    )

    op.execute(
        """
        CREATE OR REPLACE VIEW gold.dim_products AS
        SELECT
            c.id                            AS product_key,
            c.id                            AS canonical_product_id,
            c.canonical_name,
            COUNT(DISTINCT p.source)::int   AS source_count,
            MIN(p.created_at)               AS first_seen_at,
            MAX(p.updated_at)               AS last_seen_at,
            c.llm_refined_at
        FROM silver.canonical_products c
        LEFT JOIN silver.canonical_product_members m ON m.canonical_product_id = c.id
        LEFT JOIN silver.products p              ON p.id = m.silver_product_id
        GROUP BY c.id, c.canonical_name, c.llm_refined_at
        """
    )

    op.execute(
        """
        CREATE OR REPLACE VIEW gold.fact_prices AS
        SELECT
            ph.id                                        AS price_observation_id,
            m.canonical_product_id                       AS product_key,
            (TO_CHAR(ph.observed_at, 'YYYYMMDD'))::int   AS date_key,
            p.source,
            p.id                                         AS silver_product_id,
            ph.observed_at,
            ph.price,
            p.updated_at                                 AS source_last_seen_at
        FROM silver.price_history ph
        JOIN silver.products p
          ON p.id = ph.silver_product_id
        JOIN silver.canonical_product_members m
          ON m.silver_product_id = p.id
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS gold.fact_prices")
    op.execute("DROP VIEW IF EXISTS gold.dim_products")
    op.drop_table("dim_dates", schema="gold")

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
