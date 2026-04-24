from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)

from . import metadata

Table(
    "article_entities",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("silver_article_id", BigInteger, nullable=False),
    Column("entity_name", Text, nullable=False),
    Column("entity_type", String(20), nullable=False, server_default=text("'PER'")),
    Column("normalized_name", Text),
    Column("confidence", Float),
    Column("mention_count", Integer, nullable=False, server_default=text("1")),
    Column("method", String(20), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    UniqueConstraint(
        "silver_article_id",
        "entity_name",
        "method",
        name="article_entities_silver_article_id_entity_name_method_key",
    ),
    Index("idx_silver_entities_article", "silver_article_id"),
    Index("idx_silver_entities_name", "normalized_name"),
    Index("idx_silver_entities_method", "method"),
    schema="silver",
)
