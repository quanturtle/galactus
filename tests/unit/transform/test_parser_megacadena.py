import json
from datetime import datetime

from galactus.infra.db import Database
from galactus.transform.parsers.noticias.megacadena import Parser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.article import Article
from tests.unit.fakes import make_parser

RESPONSE = [
    {
        "id": 555,
        "date": "2026-03-04T09:00:00",
        "date_gmt": "2026-03-04T12:00:00",
        "link": "https://megacadena.com.py/2026/03/04/nota-de-prueba/",
        "title": {"rendered": "Nota de prueba en Megacadena"},
        "content": {"rendered": "<p>Cuerpo uno.</p><p>Cuerpo dos.</p>"},
        "_embedded": {
            "author": [{"id": 7, "name": "Redacción"}],
            "wp:featuredmedia": [{"source_url": "https://megacadena.com.py/img/cover.jpg"}],
            "wp:term": [
                [{"taxonomy": "category", "name": "Política"}],
                [{"taxonomy": "post_tag", "name": "Senado"}],
            ],
        },
    },
    {
        "id": 556,
        "date": "2026-03-04T09:30:00",
        "title": {"rendered": ""},
        "content": {"rendered": "<p>x</p>"},
        "link": "https://megacadena.com.py/2026/03/04/sin-titulo/",
    },
]


def _snapshot() -> ApiSnapshot:
    return ApiSnapshot(
        id=1,
        source="megacadena",
        source_url="https://megacadena.com.py/wp-json/wp/v2/posts?page=1",
        created_at=datetime(2026, 3, 4, 12, 0, 0),
        request_url="https://megacadena.com.py/wp-json/wp/v2/posts?page=1",
        request_params={"page": "1", "per_page": "100"},
        status_code=200,
        response_headers={},
        body=Database.compress(json.dumps(RESPONSE)),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="megacadena")


def test_build_entities_maps_posts() -> None:
    parser = _parser()
    record = _snapshot()

    articles = parser.process_record(record)

    # every post becomes an Article, even the title-less one
    assert len(articles) == 2
    article = articles[0]
    assert isinstance(article, Article)
    assert article.source == "megacadena"
    assert article.source_url == "https://megacadena.com.py/2026/03/04/nota-de-prueba/"
    assert article.title == "Nota de prueba en Megacadena"
    assert article.body == "Cuerpo uno.\n\nCuerpo dos."
    assert article.authors == ["Redacción"]
    assert article.section == "Política"
    assert article.tags == ["Senado"]
    assert article.image_urls == ["https://megacadena.com.py/img/cover.jpg"]
    assert isinstance(article.published_at, datetime)
    assert article.published_at.year == 2026
