"""rename bronze pk bronze_id to id

Revision ID: 72a47d6dcfb2
Revises: 6fc47caa4d15
Create Date: 2026-05-17 03:13:46.551155

"""
from typing import Sequence, Union

from alembic import op


revision: str = "72a47d6dcfb2"
down_revision: Union[str, Sequence[str], None] = "6fc47caa4d15"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "html_snapshots", "bronze_id", new_column_name="id", schema="bronze"
    )
    op.alter_column(
        "api_snapshots", "bronze_id", new_column_name="id", schema="bronze"
    )
    op.execute("ALTER SEQUENCE bronze.html_snapshots_bronze_id_seq RENAME TO html_snapshots_id_seq")
    op.execute("ALTER SEQUENCE bronze.api_snapshots_bronze_id_seq RENAME TO api_snapshots_id_seq")


def downgrade() -> None:
    op.execute("ALTER SEQUENCE bronze.api_snapshots_id_seq RENAME TO api_snapshots_bronze_id_seq")
    op.execute("ALTER SEQUENCE bronze.html_snapshots_id_seq RENAME TO html_snapshots_bronze_id_seq")
    op.alter_column(
        "api_snapshots", "id", new_column_name="bronze_id", schema="bronze"
    )
    op.alter_column(
        "html_snapshots", "id", new_column_name="bronze_id", schema="bronze"
    )
