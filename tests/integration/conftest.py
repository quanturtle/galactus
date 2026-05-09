"""Integration test fixtures.

Tests isolate from the dev DB's real bronze/silver data by working against a
parallel `scratch` schema. The 4 SQLAlchemy classes below mirror the production
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
from sqlalchemy import LargeBinary, Numeric, String, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import Mapped, mapped_column

# trigger psycopg3 dialect registration
import galactus.infra.db  # noqa: F401
from galactus.infra.db import Database
from sql.base import Base

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]

SCRATCH_SCHEMA = "scratch"


class ScratchApiSnapshot(Base):
    __tablename__ = "api_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "source_url",
            "created_at",
            name="uq_scratch_api_snapshots_natural_key",
        ),
        {"schema": SCRATCH_SCHEMA},
    )

    bronze_id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(index=True)
    source_url: Mapped[str] = mapped_column(index=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), index=True)
    request_url: Mapped[str]
    request_params: Mapped[dict[str, Any]] = mapped_column(JSONB)
    status_code: Mapped[int]
    response_headers: Mapped[dict[str, str]] = mapped_column(JSONB)
    body: Mapped[bytes] = mapped_column(LargeBinary)


class ScratchHtmlSnapshot(Base):
    __tablename__ = "html_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "source_url",
            "created_at",
            name="uq_scratch_html_snapshots_natural_key",
        ),
        {"schema": SCRATCH_SCHEMA},
    )

    bronze_id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(index=True)
    source_url: Mapped[str] = mapped_column(index=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), index=True)
    status_code: Mapped[int]
    content_type: Mapped[str]
    response_headers: Mapped[dict[str, str]] = mapped_column(JSONB)
    html: Mapped[bytes] = mapped_column(LargeBinary)
    is_diff: Mapped[bool] = mapped_column(default=False)


class ScratchArticle(Base):
    __tablename__ = "articles"
    __table_args__ = (
        UniqueConstraint("source", "source_url", name="uq_scratch_articles_source_url"),
        {"schema": SCRATCH_SCHEMA},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(index=True)
    source_url: Mapped[str]
    title: Mapped[str]
    body_html: Mapped[str | None] = mapped_column(default=None)
    body_text: Mapped[str | None] = mapped_column(default=None)
    authors: Mapped[list[str]] = mapped_column(ARRAY(String), server_default="{}")
    published_at: Mapped[datetime | None] = mapped_column(index=True, default=None)
    section: Mapped[str | None] = mapped_column(default=None)
    tags: Mapped[list[str]] = mapped_column(ARRAY(String), server_default="{}")
    image_urls: Mapped[list[str]] = mapped_column(ARRAY(String), server_default="{}")

    def __init__(self, **kw) -> None:
        kw.setdefault("authors", [])
        kw.setdefault("tags", [])
        kw.setdefault("image_urls", [])
        super().__init__(**kw)


class ScratchProduct(Base):
    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("source", "source_url", name="uq_scratch_products_source_url"),
        {"schema": SCRATCH_SCHEMA},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(index=True)
    source_url: Mapped[str]
    sku: Mapped[str | None] = mapped_column(default=None)
    name: Mapped[str]
    brand: Mapped[str | None] = mapped_column(default=None)
    price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), default=None)
    currency: Mapped[str | None] = mapped_column(default=None)
    unit: Mapped[str | None] = mapped_column(default=None)
    in_stock: Mapped[bool | None] = mapped_column(default=None)
    observed_at: Mapped[datetime | None] = mapped_column(index=True, default=None)
    image_urls: Mapped[list[str]] = mapped_column(ARRAY(String), server_default="{}")

    def __init__(self, **kw) -> None:
        kw.setdefault("image_urls", [])
        super().__init__(**kw)


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
        await conn.execute(
            text(
                f"TRUNCATE {SCRATCH_SCHEMA}.api_snapshots, "
                f"{SCRATCH_SCHEMA}.html_snapshots, "
                f"{SCRATCH_SCHEMA}.articles, "
                f"{SCRATCH_SCHEMA}.products RESTART IDENTITY CASCADE"
            )
        )
