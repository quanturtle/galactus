"""Integration tests for galactus.infra.db.Database against a real Postgres.

Uses the scratch-schema models defined in conftest.py — no production tables
are read or written.
"""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select, text

from tests.integration.conftest import (
    ScratchApiSnapshot,
    ScratchArticle,
    ScratchHtmlSnapshot,
    ScratchProduct,
)


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


async def _bronze_ids(engine, table: str) -> list[int]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(text(f"SELECT id FROM scratch.{table} ORDER BY id"))
        ).all()
    return [r.id for r in rows]


async def test_insert_bronze_html_records_each_fetch(db, engine) -> None:
    # Each insert is its own bronze row: id and created_at are left unset,
    # so the DB fills a fresh surrogate id and timestamp per insert.
    rec = _html_snapshot()
    await db.insert(rec, ScratchHtmlSnapshot)
    await db.insert(rec, ScratchHtmlSnapshot)
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT COUNT(*) FROM scratch.html_snapshots"))
        assert result.scalar() == 2


async def test_insert_bronze_api_records_each_fetch(db, engine) -> None:
    rec = _api_snapshot()
    await db.insert(rec, ScratchApiSnapshot)
    await db.insert(rec, ScratchApiSnapshot)
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
    await db.insert(one, ScratchHtmlSnapshot)
    await db.insert([two, three], ScratchHtmlSnapshot)
    async with engine.connect() as conn:
        count = (await conn.execute(text("SELECT COUNT(*) FROM scratch.html_snapshots"))).scalar()
    assert count == 3


async def test_insert_silver_articles_records_each_sighting(db, engine) -> None:
    # No dedup: two sightings of the same source_url from different bronze rows
    # produce two silver rows. created_at carries the bronze snapshot's timestamp.
    t1 = datetime(2026, 1, 2, 10, 0, 0)
    t2 = datetime(2026, 1, 5, 10, 0, 0)
    first = ScratchArticle(
        bronze_id=1,
        source="test_source",
        source_url="https://example.com/a-1",
        title="original",
        authors=["alice"],
        tags=["news"],
        image_urls=["https://example.com/img.jpg"],
        created_at=t1,
    )
    second = ScratchArticle(
        bronze_id=2,
        source="test_source",
        source_url="https://example.com/a-1",
        title="revised",
        authors=["alice", "bob"],
        tags=["news", "updated"],
        image_urls=["https://example.com/img.jpg"],
        created_at=t2,
    )
    await db.insert([first, second], ScratchArticle)

    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                select(ScratchArticle.__table__).order_by(ScratchArticle.__table__.c.created_at)
            )
        ).all()
    assert [(r.bronze_id, r.title, r.created_at) for r in rows] == [
        (1, "original", t1),
        (2, "revised", t2),
    ]


async def test_insert_silver_products_with_decimal(db, engine) -> None:
    t = datetime(2026, 1, 3, 12, 0, 0)
    product = ScratchProduct(
        bronze_id=7,
        source="test_source",
        source_url="https://example.com/p-1",
        name="widget",
        price=Decimal("19.99"),
        currency="USD",
        created_at=t,
    )
    await db.insert(product, ScratchProduct)
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT bronze_id, price, currency FROM scratch.products WHERE source_url = 'https://example.com/p-1'"
                )
            )
        ).one()
    assert row.bronze_id == 7
    assert row.price == Decimal("19.99")
    assert row.currency == "USD"


async def _drain_unparsed(db, bronze_model, silver_model, source):
    return [r async for r in db.stream_unparsed(bronze_model, silver_model, source)]


async def test_stream_unparsed_filters_by_source(db, engine) -> None:
    a = _html_snapshot(source="src_a", source_url="https://example.com/a")
    b = _html_snapshot(source="src_b", source_url="https://example.com/b")
    await db.insert([a, b], ScratchHtmlSnapshot)

    loaded = await _drain_unparsed(db, ScratchHtmlSnapshot, ScratchArticle, "src_b")
    assert [r.source for r in loaded] == ["src_b"]

    loaded_a = await _drain_unparsed(db, ScratchHtmlSnapshot, ScratchArticle, "src_a")
    assert [r.source for r in loaded_a] == ["src_a"]


async def test_stream_unparsed_skips_rows_already_in_silver(db, engine) -> None:
    one = _html_snapshot(source_url="https://example.com/a")
    two = _html_snapshot(source_url="https://example.com/b")
    await db.insert([one, two], ScratchHtmlSnapshot)
    first_id, second_id = await _bronze_ids(engine, "html_snapshots")

    # nothing parsed yet -> both bronze rows are unparsed
    pending = [
        r.id
        for r in await _drain_unparsed(db, ScratchHtmlSnapshot, ScratchArticle, "test_source")
    ]
    assert pending == [first_id, second_id]

    # parse the first one into a silver row carrying its (source, bronze_id)
    await db.insert(
        ScratchArticle(
            bronze_id=first_id, source="test_source", source_url="https://example.com/a", title="t"
        ),
        ScratchArticle,
    )

    # only the still-unparsed bronze row is returned now
    pending = [
        r.id
        for r in await _drain_unparsed(db, ScratchHtmlSnapshot, ScratchArticle, "test_source")
    ]
    assert pending == [second_id]


async def test_stream_unparsed_isolates_silver_rows_by_source(db, engine) -> None:
    # a silver row for a different source must not mask a bronze row that shares its id
    a = _html_snapshot(source="src_a", source_url="https://example.com/a")
    await db.insert(a, ScratchHtmlSnapshot)
    (bronze_id,) = await _bronze_ids(engine, "html_snapshots")
    await db.insert(
        ScratchArticle(
            bronze_id=bronze_id, source="src_b", source_url="https://example.com/x", title="t"
        ),
        ScratchArticle,
    )

    pending = [
        r.id for r in await _drain_unparsed(db, ScratchHtmlSnapshot, ScratchArticle, "src_a")
    ]
    assert pending == [bronze_id]
