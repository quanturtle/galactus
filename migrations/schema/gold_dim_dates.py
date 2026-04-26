from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Integer,
    SmallInteger,
    Table,
    Text,
    UniqueConstraint,
)

from . import metadata

Table(
    "dim_dates",
    metadata,
    Column("date_key", Integer, primary_key=True),
    Column("date", Date, nullable=False),
    Column("year", SmallInteger, nullable=False),
    Column("quarter", SmallInteger, nullable=False),
    Column("month", SmallInteger, nullable=False),
    Column("month_name", Text, nullable=False),
    Column("day", SmallInteger, nullable=False),
    Column("day_of_week", SmallInteger, nullable=False),
    Column("day_name", Text, nullable=False),
    Column("week_of_year", SmallInteger, nullable=False),
    Column("is_weekend", Boolean, nullable=False),
    UniqueConstraint("date", name="uq_dim_dates_date"),
    schema="gold",
)
