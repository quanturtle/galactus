from datetime import datetime
from typing import Any

from sqlalchemy import LargeBinary, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from sql.a_bronze.schema import SCHEMA
from sql.base import Base


class ApiSnapshot(Base):
    """bronze.api_snapshots — raw API response captures."""

    __tablename__ = "api_snapshots"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(index=True)
    source_url: Mapped[str] = mapped_column(index=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), index=True)
    request_url: Mapped[str]
    request_params: Mapped[dict[str, Any]] = mapped_column(JSONB)
    status_code: Mapped[int]
    response_headers: Mapped[dict[str, str]] = mapped_column(JSONB)
    body: Mapped[bytes] = mapped_column(LargeBinary)
