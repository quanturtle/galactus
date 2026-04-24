from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Index,
    LargeBinary,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from . import metadata

Table(
    "api_responses",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("source", Text, nullable=False),
    Column("endpoint", Text, nullable=False),
    Column("page_params", JSONB),
    Column("response_blob", LargeBinary, nullable=False),
    Column("fetch_date", Date, nullable=False, server_default=text("CURRENT_DATE")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("NOW()")),
    Column("parsed_at", DateTime(timezone=True)),
    Column("images_processed_at", DateTime(timezone=True)),
    UniqueConstraint(
        "source", "endpoint", "fetch_date", name="uq_api_responses_source_endpoint_date"
    ),
    Index("idx_api_responses_unparsed", "source", postgresql_where=text("parsed_at IS NULL")),
    Index(
        "idx_api_responses_images_pending",
        "source",
        postgresql_where=text("images_processed_at IS NULL"),
    ),
    schema="bronze",
)
