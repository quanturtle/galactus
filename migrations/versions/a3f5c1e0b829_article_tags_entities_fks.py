"""article_tags and article_entities foreign keys

Revision ID: a3f5c1e0b829
Revises: d7f1e8a4c9b2
Create Date: 2026-04-24 00:00:00.000000

Adds the missing ``silver_article_id`` foreign keys on ``silver.article_tags``
and ``silver.article_entities`` (both already declare the column NOT NULL but
had no FK — child rows of a deleted article stayed orphaned). Matches the
``ON DELETE CASCADE`` convention used by ``silver.article_images`` and
``silver.product_images``.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "a3f5c1e0b829"
down_revision: Union[str, Sequence[str], None] = "d7f1e8a4c9b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_foreign_key(
        "fk_silver_article_tags_silver_article_id_articles",
        "article_tags",
        "articles",
        ["silver_article_id"],
        ["id"],
        source_schema="silver",
        referent_schema="silver",
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_silver_article_entities_silver_article_id_articles",
        "article_entities",
        "articles",
        ["silver_article_id"],
        ["id"],
        source_schema="silver",
        referent_schema="silver",
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_silver_article_entities_silver_article_id_articles",
        "article_entities",
        schema="silver",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_silver_article_tags_silver_article_id_articles",
        "article_tags",
        schema="silver",
        type_="foreignkey",
    )
