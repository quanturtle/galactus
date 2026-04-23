"""gold counts view

Revision ID: b4d2e7f1a3c9
Revises: 8386d58dc45a
Create Date: 2026-04-23 06:40:00.000000

Creates the ``gold`` schema and a ``gold.counts`` view that reports per-source
bronze and silver row counts. Type is inferred from the silver table the source
lands in (``silver.articles`` -> noticias, ``silver.products`` -> supermercados).
"""
from typing import Sequence, Union

from alembic import op


revision: str = "b4d2e7f1a3c9"
down_revision: Union[str, Sequence[str], None] = "8386d58dc45a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS gold")
    op.execute(
        """
        CREATE OR REPLACE VIEW gold.counts AS
        WITH bronze_by_source AS (
            SELECT source, COUNT(*)::bigint AS bronze_count
            FROM bronze.snapshots GROUP BY source
            UNION ALL
            SELECT source, COUNT(*)::bigint AS bronze_count
            FROM bronze.api_responses GROUP BY source
        ),
        bronze_agg AS (
            SELECT source, SUM(bronze_count)::bigint AS bronze_count
            FROM bronze_by_source
            GROUP BY source
        ),
        silver_by_source AS (
            SELECT 'noticias'::text AS type, source, COUNT(*)::bigint AS silver_count
            FROM silver.articles GROUP BY source
            UNION ALL
            SELECT 'supermercados'::text AS type, source, COUNT(*)::bigint AS silver_count
            FROM silver.products GROUP BY source
        )
        SELECT s.type,
               s.source,
               COALESCE(b.bronze_count, 0) AS bronze_count,
               s.silver_count
        FROM silver_by_source s
        LEFT JOIN bronze_agg b ON b.source = s.source
        ORDER BY s.type, s.source
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS gold.counts")
    op.execute("DROP SCHEMA IF EXISTS gold")
