"""Integration test fixtures.

Tests isolate from the dev DB's real bronze/silver data by working against a
parallel `scratch` schema. The 4 SQLModel classes below mirror the production
column shapes but live under schema=scratch; the session fixture creates the
schema and tables, and drops them at teardown.

Tests assume the dev Postgres is up (`docker compose up -d db`); they do not
require galactus-migrate to have run because they create their own tables.
"""

import os
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest_asyncio
from dotenv import load_dotenv
from sqlalchemy import LargeBinary, Numeric, String, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import Column, Field, SQLModel

# trigger psycopg3 dialect registration
import galactus.infra.db  # noqa: F401
from galactus.infra.db import Database

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]

SCRATCH_SCHEMA = "scratch"


class ScratchApiSnapshot(SQLModel, table=True):
    __tablename__ = "api_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "source", "source_url", "fetched_at",
            name="uq_scratch_api_snapshots_natural_key",
        ),
        {"schema": SCRATCH_SCHEMA},
    )

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


class ScratchHtmlSnapshot(SQLModel, table=True):
    __tablename__ = "html_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "source", "source_url", "fetched_at",
            name="uq_scratch_html_snapshots_natural_key",
        ),
        {"schema": SCRATCH_SCHEMA},
    )

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


class ScratchArticle(SQLModel, table=True):
    __tablename__ = "articles"
    __table_args__ = (
        UniqueConstraint("source", "source_url", name="uq_scratch_articles_source_url"),
        {"schema": SCRATCH_SCHEMA},
    )

    id: int | None = Field(default=None, primary_key=True)
    source: str = Field(index=True)
    source_url: str
    title: str
    body_html: str | None = None
    body_text: str | None = None
    authors: list[str] = Field(
        default_factory=list,
        sa_column=Column(ARRAY(String), nullable=False, server_default="{}"),
    )
    published_at: datetime | None = Field(default=None, index=True)
    section: str | None = None
    tags: list[str] = Field(
        default_factory=list,
        sa_column=Column(ARRAY(String), nullable=False, server_default="{}"),
    )
    image_urls: list[str] = Field(
        default_factory=list,
        sa_column=Column(ARRAY(String), nullable=False, server_default="{}"),
    )


class ScratchProduct(SQLModel, table=True):
    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("source", "source_url", name="uq_scratch_products_source_url"),
        {"schema": SCRATCH_SCHEMA},
    )

    id: int | None = Field(default=None, primary_key=True)
    source: str = Field(index=True)
    source_url: str
    sku: str | None = None
    name: str
    brand: str | None = None
    price: Decimal | None = Field(default=None, sa_column=Column(Numeric(12, 2)))
    currency: str | None = None
    unit: str | None = None
    in_stock: bool | None = None
    observed_at: datetime | None = Field(default=None, index=True)
    image_urls: list[str] = Field(
        default_factory=list,
        sa_column=Column(ARRAY(String), nullable=False, server_default="{}"),
    )


SCRATCH_TABLES = (
    ScratchApiSnapshot.__table__,
    ScratchHtmlSnapshot.__table__,
    ScratchArticle.__table__,
    ScratchProduct.__table__,
)


@pytest_asyncio.fixture(scope="session")
async def engine() -> AsyncEngine:
    eng = create_async_engine(DATABASE_URL)
    async with eng.begin() as conn:
        await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCRATCH_SCHEMA}"))
        for table in SCRATCH_TABLES:
            await conn.run_sync(table.create, checkfirst=True)
    yield eng
    async with eng.begin() as conn:
        await conn.execute(text(f"DROP SCHEMA IF EXISTS {SCRATCH_SCHEMA} CASCADE"))
    await eng.dispose()


@pytest_asyncio.fixture
async def db(engine) -> Database:
    """Database instance bound to the same DSN as the engine fixture."""
    instance = Database(DATABASE_URL)
    await instance.open()
    try:
        yield instance
    finally:
        await instance.close()


@pytest_asyncio.fixture(autouse=True)
async def _truncate(engine):
    yield
    async with engine.begin() as conn:
        await conn.execute(text(
            f"TRUNCATE {SCRATCH_SCHEMA}.api_snapshots, "
            f"{SCRATCH_SCHEMA}.html_snapshots, "
            f"{SCRATCH_SCHEMA}.articles, "
            f"{SCRATCH_SCHEMA}.products RESTART IDENTITY CASCADE"
        ))
