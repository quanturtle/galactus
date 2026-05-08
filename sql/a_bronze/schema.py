"""Registers bronze schema creation as a metadata-level DDL hook."""

from sqlalchemy import DDL, event

from sql.base import Base

SCHEMA = "bronze"

event.listen(
    Base.metadata,
    "before_create",
    DDL(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"),
)
