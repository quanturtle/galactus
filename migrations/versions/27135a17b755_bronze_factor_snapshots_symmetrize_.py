"""bronze: factor snapshots, symmetrize columns

Revision ID: 27135a17b755
Revises: d9d2543af773
Create Date: 2026-05-19 11:12:38.723923

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '27135a17b755'
down_revision: Union[str, Sequence[str], None] = 'd9d2543af773'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # html_snapshots: rename source_url -> request_url (data preserved)
    op.drop_index(
        op.f("ix_bronze_html_snapshots_source_url"),
        table_name="html_snapshots",
        schema="bronze",
    )
    op.alter_column(
        "html_snapshots",
        "source_url",
        new_column_name="request_url",
        existing_type=sa.String(),
        existing_nullable=False,
        schema="bronze",
    )
    op.create_index(
        op.f("ix_bronze_html_snapshots_request_url"),
        "html_snapshots",
        ["request_url"],
        unique=False,
        schema="bronze",
    )

    # html_snapshots: rename html -> body (data preserved)
    op.alter_column(
        "html_snapshots",
        "html",
        new_column_name="body",
        existing_type=sa.LargeBinary(),
        existing_nullable=False,
        schema="bronze",
    )

    # html_snapshots: backfill empty-jsonb defaults for new NOT NULL columns,
    # then drop the server_default so the schema matches the model.
    op.add_column(
        "html_snapshots",
        sa.Column(
            "request_headers",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        schema="bronze",
    )
    op.alter_column(
        "html_snapshots",
        "request_headers",
        server_default=None,
        schema="bronze",
    )
    op.add_column(
        "html_snapshots",
        sa.Column(
            "request_params",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        schema="bronze",
    )
    op.alter_column(
        "html_snapshots",
        "request_params",
        server_default=None,
        schema="bronze",
    )

    # html_snapshots: drop deferred is_diff column
    op.drop_column("html_snapshots", "is_diff", schema="bronze")

    # api_snapshots: source_url duplicated request_url's data; drop it
    op.drop_index(
        op.f("ix_bronze_api_snapshots_source_url"),
        table_name="api_snapshots",
        schema="bronze",
    )
    op.drop_column("api_snapshots", "source_url", schema="bronze")
    op.create_index(
        op.f("ix_bronze_api_snapshots_request_url"),
        "api_snapshots",
        ["request_url"],
        unique=False,
        schema="bronze",
    )

    # api_snapshots: add request_headers + content_type with backfill defaults
    op.add_column(
        "api_snapshots",
        sa.Column(
            "request_headers",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        schema="bronze",
    )
    op.alter_column(
        "api_snapshots",
        "request_headers",
        server_default=None,
        schema="bronze",
    )
    op.add_column(
        "api_snapshots",
        sa.Column(
            "content_type",
            sa.String(),
            nullable=False,
            server_default=sa.text("''"),
        ),
        schema="bronze",
    )
    # backfill content_type from response_headers->>'content-type' where present
    op.execute(
        "UPDATE bronze.api_snapshots "
        "SET content_type = COALESCE(response_headers->>'content-type', '')"
    )
    op.alter_column(
        "api_snapshots",
        "content_type",
        server_default=None,
        schema="bronze",
    )


def downgrade() -> None:
    """Downgrade schema."""
    # api_snapshots: restore source_url by copying from request_url
    op.add_column(
        "api_snapshots",
        sa.Column(
            "source_url",
            sa.VARCHAR(),
            autoincrement=False,
            nullable=False,
            server_default=sa.text("''"),
        ),
        schema="bronze",
    )
    op.execute("UPDATE bronze.api_snapshots SET source_url = request_url")
    op.alter_column("api_snapshots", "source_url", server_default=None, schema="bronze")
    op.drop_index(
        op.f("ix_bronze_api_snapshots_request_url"),
        table_name="api_snapshots",
        schema="bronze",
    )
    op.create_index(
        op.f("ix_bronze_api_snapshots_source_url"),
        "api_snapshots",
        ["source_url"],
        unique=False,
        schema="bronze",
    )
    op.drop_column("api_snapshots", "content_type", schema="bronze")
    op.drop_column("api_snapshots", "request_headers", schema="bronze")

    # html_snapshots: re-add is_diff with default false
    op.add_column(
        "html_snapshots",
        sa.Column(
            "is_diff",
            sa.BOOLEAN(),
            autoincrement=False,
            nullable=False,
            server_default=sa.text("false"),
        ),
        schema="bronze",
    )
    op.alter_column("html_snapshots", "is_diff", server_default=None, schema="bronze")

    # html_snapshots: drop the new JSONB columns
    op.drop_column("html_snapshots", "request_params", schema="bronze")
    op.drop_column("html_snapshots", "request_headers", schema="bronze")

    # html_snapshots: rename body -> html, request_url -> source_url
    op.alter_column(
        "html_snapshots",
        "body",
        new_column_name="html",
        existing_type=sa.LargeBinary(),
        existing_nullable=False,
        schema="bronze",
    )
    op.drop_index(
        op.f("ix_bronze_html_snapshots_request_url"),
        table_name="html_snapshots",
        schema="bronze",
    )
    op.alter_column(
        "html_snapshots",
        "request_url",
        new_column_name="source_url",
        existing_type=sa.String(),
        existing_nullable=False,
        schema="bronze",
    )
    op.create_index(
        op.f("ix_bronze_html_snapshots_source_url"),
        "html_snapshots",
        ["source_url"],
        unique=False,
        schema="bronze",
    )
