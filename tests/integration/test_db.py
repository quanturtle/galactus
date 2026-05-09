"""Integration tests for galactus.infra.db.Database against a real Postgres.

Uses the scratch-schema models defined in conftest.py — no production tables
are read or written.
"""

from decimal import Decimal

from sqlalchemy import select, text

from tests.integration.conftest import (
    ScratchApiSnapshot,
    ScratchArticle,
    ScratchHtmlSnapshot,
    ScratchProduct,
)

BRONZE_CONFLICT = ("source", "source_url", "created_at")
BRONZE_EXCLUDE = ("bronze_id", "created_at")
SILVER_CONFLICT = ("source", "source_url")
SILVER_EXCLUDE = ("id",)


def _api_snapshot(
    source: str = "test_source",
    source_url: str = "https://example.com/api/1",
) -> ScratchApiSnapshot:
    return ScratchApiSnapshot(
        source=source,
        source_url=source_url,
        request_url="https://example.com/api/1?page=1",
        request_params={"page": 1, "size": 10},
        status_code=200,
        response_headers={"content-type": "application/json"},
        body=b'{"items": []}',
    )


def _html_snapshot(
    source: str = "test_source",
    source_url: str = "https://example.com/article-1",
) -> ScratchHtmlSnapshot:
    return ScratchHtmlSnapshot(
        source=source,
        source_url=source_url,
        status_code=200,
        content_type="text/html; charset=utf-8",
        response_headers={"content-type": "text/html"},
        html=b"<html><body>hello</body></html>",
    )


async def test_insert_bronze_html_records_each_fetch(db, engine) -> None:
    # Two fetches of the same URL produce two bronze rows: created_at is server-
    # filled per insert, so the (source, source_url, created_at) natural key differs.
    rec = _html_snapshot()
    await db.insert(rec, ScratchHtmlSnapshot, BRONZE_CONFLICT, BRONZE_EXCLUDE)
    await db.insert(rec, ScratchHtmlSnapshot, BRONZE_CONFLICT, BRONZE_EXCLUDE)
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT COUNT(*) FROM scratch.html_snapshots"))
        assert result.scalar() == 2


async def test_insert_bronze_api_records_each_fetch(db, engine) -> None:
    rec = _api_snapshot()
    await db.insert(rec, ScratchApiSnapshot, BRONZE_CONFLICT, BRONZE_EXCLUDE)
    await db.insert(rec, ScratchApiSnapshot, BRONZE_CONFLICT, BRONZE_EXCLUDE)
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text("SELECT request_params, response_headers FROM scratch.api_snapshots")
            )
        ).all()
    assert len(rows) == 2
    for row in rows:
        assert row.request_params == {"page": 1, "size": 10}
        assert row.response_headers == {"content-type": "application/json"}


async def test_insert_accepts_single_or_iterable(db, engine) -> None:
    one = _html_snapshot(source_url="https://example.com/a")
    two = _html_snapshot(source_url="https://example.com/b")
    three = _html_snapshot(source_url="https://example.com/c")
    await db.insert(one, ScratchHtmlSnapshot, BRONZE_CONFLICT, BRONZE_EXCLUDE)
    await db.insert([two, three], ScratchHtmlSnapshot, BRONZE_CONFLICT, BRONZE_EXCLUDE)
    async with engine.connect() as conn:
        count = (await conn.execute(text("SELECT COUNT(*) FROM scratch.html_snapshots"))).scalar()
    assert count == 3


async def test_upsert_silver_articles_creates_then_updates(db, engine) -> None:
    article = ScratchArticle(
        source="test_source",
        source_url="https://example.com/a-1",
        title="original",
        authors=["alice"],
        tags=["news"],
        image_urls=["https://example.com/img.jpg"],
    )
    await db.upsert(article, ScratchArticle, SILVER_CONFLICT, SILVER_EXCLUDE)

    updated = ScratchArticle(
        source="test_source",
        source_url="https://example.com/a-1",
        title="revised",
        authors=["alice", "bob"],
        tags=["news", "updated"],
        image_urls=["https://example.com/img.jpg"],
    )
    await db.upsert(updated, ScratchArticle, SILVER_CONFLICT, SILVER_EXCLUDE)

    async with engine.connect() as conn:
        rows = (await conn.execute(select(ScratchArticle.__table__))).all()
    assert len(rows) == 1
    assert rows[0].title == "revised"
    assert rows[0].authors == ["alice", "bob"]


async def test_upsert_silver_products_with_decimal(db, engine) -> None:
    product = ScratchProduct(
        source="test_source",
        source_url="https://example.com/p-1",
        name="widget",
        price=Decimal("19.99"),
        currency="USD",
    )
    await db.upsert(product, ScratchProduct, SILVER_CONFLICT, SILVER_EXCLUDE)
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT price, currency FROM scratch.products WHERE source_url = 'https://example.com/p-1'"
                )
            )
        ).one()
    assert row.price == Decimal("19.99")
    assert row.currency == "USD"


async def test_load_for_source_filters_by_source(db, engine) -> None:
    a = _html_snapshot(source="src_a", source_url="https://example.com/a")
    b = _html_snapshot(source="src_b", source_url="https://example.com/b")
    await db.insert([a, b], ScratchHtmlSnapshot, BRONZE_CONFLICT, BRONZE_EXCLUDE)

    yielded = [r async for r in db.load_for_source(ScratchHtmlSnapshot, "src_b")]
    assert len(yielded) == 1
    assert yielded[0].source == "src_b"

    yielded_a = [r async for r in db.load_for_source(ScratchHtmlSnapshot, "src_a")]
    assert len(yielded_a) == 1
    assert yielded_a[0].source == "src_a"


async def test_load_for_source_full_rescan(db, engine) -> None:
    one = _html_snapshot(source_url="https://example.com/a")
    two = _html_snapshot(source_url="https://example.com/b")
    await db.insert([one, two], ScratchHtmlSnapshot, BRONZE_CONFLICT, BRONZE_EXCLUDE)

    first = [r async for r in db.load_for_source(ScratchHtmlSnapshot, "test_source")]
    assert len(first) == 2

    # idempotent re-scan: the same rows are yielded on every call
    second = [r async for r in db.load_for_source(ScratchHtmlSnapshot, "test_source")]
    assert [r.bronze_id for r in second] == [r.bronze_id for r in first]
