from datetime import datetime

from sqlalchemy import LargeBinary
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, Field, SQLModel

from sql.a_bronze.schema import SCHEMA


class HtmlSnapshot(SQLModel, table=True):
    """bronze.html_snapshots — first-full-then-diff HTML captures."""

    __tablename__ = "html_snapshots"
    __table_args__ = {"schema": SCHEMA}

    bronze_id: int | None = Field(default=None, primary_key=True)
    source: str = Field(index=True)
    source_url: str = Field(index=True)
    fetched_at: datetime = Field(index=True)
    status_code: int
    content_type: str
    response_headers: dict[str, str] = Field(sa_column=Column(JSONB, nullable=False))
    html: bytes = Field(sa_column=Column(LargeBinary, nullable=False))
    is_diff: bool = Field(default=False)
    parsed_at: datetime | None = Field(default=None, index=True)
