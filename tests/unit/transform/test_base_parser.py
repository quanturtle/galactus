import asyncio
import json
from datetime import datetime
from typing import Any

import pytest
from bs4 import BeautifulSoup

from galactus.core.errors import DatabaseError, ParserError
from galactus.transform.base_parser import BaseParser
from galactus.infra.db import Database
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.article import Article
from sql.base import Base
from tests.unit.fakes import FakeDatabase, make_parser, make_transform_config


class _StubParser(BaseParser):
    """BaseParser stub with build_entity inlined to keep the test isolated from the mixins."""

    bronze_model = HtmlSnapshot
    silver_model = Article

    def build_entity(self, item: Any) -> Base:
        return Article(
            source=self.source,
            source_url="https://example.test/a",
            title="title",
        )


class _WiredStubParser(_StubParser):
    """_StubParser that routes a pre-set fake db through make_database."""

    wired_db: FakeDatabase

    def make_database(self) -> FakeDatabase:  # type: ignore[override]
        return self.wired_db


def _html_snapshot(html: str, bronze_id: int = 1) -> HtmlSnapshot:
    return HtmlSnapshot(
        id=bronze_id,
        source="testsrc",
        source_url="https://example.test/a",
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        status_code=200,
        content_type="text/html",
        response_headers={},
        html=Database.compress(html),
        is_diff=False,
    )


def _api_snapshot(payload: Any, bronze_id: int = 1) -> ApiSnapshot:
    return ApiSnapshot(
        id=bronze_id,
        source="testsrc",
        source_url="https://example.test/api",
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        request_url="https://example.test/api?p=1",
        request_params={"p": 1},
        status_code=200,
        response_headers={},
        body=Database.compress(json.dumps(payload)),
    )


def test_decode_html_snapshot_returns_beautifulsoup() -> None:
    parser = make_parser(_StubParser)
    record = _html_snapshot("<html><body><p>hi</p></body></html>")

    decoded = parser.decode(record)

    assert isinstance(decoded, BeautifulSoup)
    assert decoded.find("p").get_text() == "hi"


def test_decode_api_snapshot_returns_dict() -> None:
    class ApiStubParser(_StubParser):
        bronze_model = ApiSnapshot

    parser = make_parser(ApiStubParser)
    record = _api_snapshot({"k": 1})

    decoded = parser.decode(record)

    assert decoded == {"k": 1}


def test_process_record_stamps_bronze_provenance() -> None:
    class MultiEntityParser(_StubParser):
        def build_item(self, decoded: Any) -> list[Any]:
            return [
                ("https://example.test/1", "one"),
                ("https://example.test/2", "two"),
            ]

        def build_entity(self, item: Any) -> Base:
            source_url, title = item
            return Article(source=self.source, source_url=source_url, title=title)

    parser = make_parser(MultiEntityParser)
    record = _html_snapshot("<html></html>", bronze_id=42)
    record.created_at = datetime(2026, 3, 14, 9, 0, 0)

    entities = parser.process_record(record)

    assert len(entities) == 2
    for entity in entities:
        assert entity.bronze_id == 42
        assert entity.created_at == datetime(2026, 3, 14, 9, 0, 0)


def test_process_record_wraps_subclass_errors_as_parsererror() -> None:
    class BoomParser(_StubParser):
        def build_item(self, decoded: Any) -> list[Any]:
            raise ValueError("boom")

    parser = make_parser(BoomParser)
    record = _html_snapshot("<html></html>", bronze_id=7)

    with pytest.raises(ParserError) as exc_info:
        parser.process_record(record)

    assert "7" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_process_record_passes_parsererror_through() -> None:
    sentinel = ParserError("explicit")

    class ExplicitParser(_StubParser):
        def build_item(self, decoded: Any) -> list[Any]:
            raise sentinel

    parser = make_parser(ExplicitParser)
    record = _html_snapshot("<html></html>")

    with pytest.raises(ParserError) as exc_info:
        parser.process_record(record)

    assert exc_info.value is sentinel


def test_run_executes_load_then_parse_then_insert() -> None:
    records = [
        _html_snapshot("<html><p>one</p></html>", bronze_id=1),
        _html_snapshot("<html><p>two</p></html>", bronze_id=2),
    ]
    db = FakeDatabase(load_unparsed_results=records)
    parser = _WiredStubParser(make_transform_config(source="testsrc"))
    parser.wired_db = db

    asyncio.run(parser.run())

    assert db.load_calls == [(HtmlSnapshot, Article, "testsrc")]
    assert len(db.inserts) == 2
    assert all(model is Article for _, model in db.inserts)
    assert {entity.bronze_id for entity, _ in db.inserts} == {1, 2}


def test_run_inserts_after_each_bronze_record() -> None:
    records = [
        _html_snapshot("<html><p>one</p></html>", bronze_id=1),
        _html_snapshot("<html><p>two</p></html>", bronze_id=2),
        _html_snapshot("<html><p>three</p></html>", bronze_id=3),
    ]
    db = FakeDatabase(load_unparsed_results=records)
    parser = _WiredStubParser(make_transform_config(source="testsrc"))
    parser.wired_db = db

    asyncio.run(parser.run())

    assert db.insert_call_count == 3


def test_run_wraps_database_error_as_parsererror() -> None:
    db_error = DatabaseError("insert failed")
    db = FakeDatabase(
        load_unparsed_results=[_html_snapshot("<html></html>")],
        insert_raises=db_error,
    )
    parser = _WiredStubParser(make_transform_config(source="testsrc"))
    parser.wired_db = db

    with pytest.raises(ParserError) as exc_info:
        asyncio.run(parser.run())

    assert exc_info.value.__cause__ is db_error
