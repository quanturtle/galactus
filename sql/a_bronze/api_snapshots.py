from datetime import datetime
from typing import Any

from sqlalchemy import LargeBinary
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, Field, SQLModel

from sql.a_bronze.schema import SCHEMA


class ApiSnapshot(SQLModel, table=True):
    """bronze.api_snapshots — raw API response captures."""

    __tablename__ = "api_snapshots"
    __table_args__ = {"schema": SCHEMA}

    bronze_id: int | None = Field(default=None, primary_key=True)
    source: str = Field(index=True)
    source_url: str = Field(index=True)
    fetched_at: datetime = Field(index=True)
    request_url: str
    request_params: dict[str, Any] = Field(sa_column=Column(JSONB, nullable=False))
    status_code: int
    response_headers: dict[str, str] = Field(sa_column=Column(JSONB, nullable=False))
    body: bytes = Field(sa_column=Column(LargeBinary, nullable=False))
    parsed_at: datetime | None = Field(default=None, index=True)
