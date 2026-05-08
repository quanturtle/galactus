"""Registers bronze schema creation as a metadata-level DDL hook."""

from sqlalchemy import DDL, event
from sqlmodel import SQLModel

SCHEMA = "bronze"

event.listen(
    SQLModel.metadata,
    "before_create",
    DDL(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"),
)
