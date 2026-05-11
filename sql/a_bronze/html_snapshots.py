from datetime import datetime

from sqlalchemy import LargeBinary, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from sql.a_bronze.schema import SCHEMA
from sql.base import Base


class HtmlSnapshot(Base):
    """bronze.html_snapshots — first-full-then-diff HTML captures."""

    __tablename__ = "html_snapshots"
    __table_args__ = {"schema": SCHEMA}

    bronze_id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(index=True)
    source_url: Mapped[str] = mapped_column(index=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), index=True)
    status_code: Mapped[int]
    content_type: Mapped[str]
    response_headers: Mapped[dict[str, str]] = mapped_column(JSONB)
    html: Mapped[bytes] = mapped_column(LargeBinary)
    is_diff: Mapped[bool] = mapped_column(default=False)
