import json
from datetime import datetime

from galactus.infra.db import Database
from galactus.transform.parsers.noticias.hoy import Parser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.article import Article
from tests.unit.fakes import make_parser

RESPONSE = [
    {
        "id": 12345,
        "date": "2026-01-02T07:30:00",
        "date_gmt": "2026-01-02T10:30:00",
        "link": "https://www.hoy.com.py/nacionales/articulo-importante",
        "title": {"rendered": "Artículo importante de hoy"},
        "content": {
            "rendered": ("<p>Primer párrafo del cuerpo.</p><p>Segundo párrafo con detalle.</p>")
        },
        "_embedded": {
            "author": [{"id": 1, "name": "Juan Pérez"}, {"id": 2, "name": "María Gómez"}],
            "wp:featuredmedia": [
                {"source_url": "https://www.hoy.com.py/img/hero.jpg"},
                {"source_url": "https://www.hoy.com.py/img/hero.jpg"},
            ],
            "wp:term": [
                [{"taxonomy": "category", "name": "Nacionales"}],
                [
                    {"taxonomy": "post_tag", "name": "Política"},
                    {"taxonomy": "post_tag", "name": "Gobierno"},
                ],
            ],
        },
    },
    {
        "id": 12346,
        "date": "2026-01-02T08:00:00",
        "title": {"rendered": ""},
        "content": {"rendered": "<p>nope</p>"},
        "link": "https://www.hoy.com.py/nada",
    },
]


def _snapshot() -> ApiSnapshot:
    return ApiSnapshot(
        id=1,
        source="hoy",
        source_url="https://www.hoy.com.py/wp-json/wp/v2/posts?page=1",
        created_at=datetime(2026, 1, 2, 10, 0, 0),
        request_url="https://www.hoy.com.py/wp-json/wp/v2/posts?page=1",
        request_params={"page": "1", "per_page": "100"},
        status_code=200,
        response_headers={},
        body=Database.compress(json.dumps(RESPONSE)),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="hoy")


def test_build_entities_maps_posts() -> None:
    parser = _parser()
    record = _snapshot()

    articles = parser.process_record(record)

    # every post becomes an Article, even the title-less one
    assert len(articles) == 2
    article = articles[0]
    assert isinstance(article, Article)
    assert article.source == "hoy"
    assert article.source_url == "https://www.hoy.com.py/nacionales/articulo-importante"
    assert article.title == "Artículo importante de hoy"
    assert article.body == "Primer párrafo del cuerpo.\n\nSegundo párrafo con detalle."
    assert article.authors == ["Juan Pérez", "María Gómez"]
    assert article.section == "Nacionales"
    assert article.tags == ["Política", "Gobierno"]
    # featured image deduped
    assert article.image_urls == ["https://www.hoy.com.py/img/hero.jpg"]
    # date_gmt parsed
    assert isinstance(article.published_at, datetime)
    assert article.published_at.year == 2026
    assert article.published_at.month == 1
    assert article.published_at.day == 2


def test_empty_array_returns_no_entities() -> None:
    parser = _parser()
    record = ApiSnapshot(
        id=2,
        source="hoy",
        source_url="https://www.hoy.com.py/wp-json/wp/v2/posts?page=999",
        created_at=datetime(2026, 1, 2, 10, 0, 0),
        request_url="https://www.hoy.com.py/wp-json/wp/v2/posts?page=999",
        request_params={"page": "999"},
        status_code=200,
        response_headers={},
        body=Database.compress(json.dumps([])),
    )

    assert parser.process_record(record) == []
