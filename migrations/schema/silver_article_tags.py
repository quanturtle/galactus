from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY

from . import metadata

Table(
    "article_tags",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column(
        "silver_article_id",
        BigInteger,
        ForeignKey(
            "silver.articles.id",
            ondelete="CASCADE",
            name="fk_silver_article_tags_silver_article_id_articles",
        ),
        nullable=False,
    ),
    Column("tags", ARRAY(Text), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    UniqueConstraint("silver_article_id", name="article_tags_silver_article_id_key"),
    Index("idx_silver_tags_article", "silver_article_id"),
    Index("idx_silver_tags_gin", "tags", postgresql_using="gin"),
    schema="silver",
)
