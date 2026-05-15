import json
from datetime import datetime

from galactus.transform.html_parser import compress
from galactus.transform.parsers.noticias.abc_color import Parser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.article import Article
from tests.unit.fakes import make_parser

RESPONSE = {
    "content_elements": [
        {
            "_id": "ABCDEFG",
            "type": "story",
            "canonical_url": "/nacionales/2026/01/02/articulo-de-prueba/",
            "headlines": {"basic": "Artículo de prueba"},
            "publish_date": "2026-01-02T10:30:00Z",
            "credits": {
                "by": [
                    {"name": "Juan Pérez", "type": "author"},
                    {"name": "María Gómez", "type": "author"},
                ]
            },
            "taxonomy": {
                "primary_section": {"name": "Nacionales"},
                "sections": [{"name": "Nacionales"}, {"name": "Política"}],
                "tags": [{"text": "Senado"}, {"text": "Reforma"}],
            },
            "promo_items": {
                "basic": {
                    "type": "image",
                    "url": "https://www.abc.com.py/img/promo.jpg",
                }
            },
            "content_elements": [
                {"type": "text", "content": "<p>Primer párrafo del cuerpo.</p>"},
                {"type": "image", "url": "https://www.abc.com.py/img/inline.jpg"},
                {"type": "text", "content": "Segundo párrafo plano."},
            ],
        },
        {
            "_id": "NOTITLE",
            "type": "story",
            "canonical_url": "/nada/",
            "headlines": {"basic": ""},
        },
    ]
}


def _snapshot() -> ApiSnapshot:
    return ApiSnapshot(
        bronze_id=1,
        source="abc_color",
        source_url="https://www.abc.com.py/pf/api/v3/content/fetch/sections-api",
        created_at=datetime(2026, 1, 2, 10, 0, 0),
        request_url="https://www.abc.com.py/pf/api/v3/content/fetch/sections-api",
        request_params={"_website": "abc-color"},
        status_code=200,
        response_headers={},
        body=compress(json.dumps(RESPONSE)),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="abc_color")


def test_build_entities_maps_stories() -> None:
    parser = _parser()
    record = _snapshot()

    articles = parser.process_record(record)

    # every story becomes an Article, even the headline-less one
    assert len(articles) == 2
    article = articles[0]
    assert isinstance(article, Article)
    assert article.source == "abc_color"
    assert article.source_url == (
        "https://www.abc.com.py/nacionales/2026/01/02/articulo-de-prueba/"
    )
    assert article.title == "Artículo de prueba"
    assert article.body == "Primer párrafo del cuerpo.\n\nSegundo párrafo plano."
    assert article.authors == ["Juan Pérez", "María Gómez"]
    # primary_section wins over sections[]
    assert article.section == "Nacionales"
    assert article.tags == ["Senado", "Reforma"]
    # promo image first, then inline image
    assert article.image_urls == [
        "https://www.abc.com.py/img/promo.jpg",
        "https://www.abc.com.py/img/inline.jpg",
    ]
    assert isinstance(article.published_at, datetime)
    assert article.published_at.year == 2026
    assert article.published_at.month == 1
    assert article.published_at.day == 2


def test_empty_feed_returns_no_entities() -> None:
    parser = _parser()
    record = ApiSnapshot(
        bronze_id=2,
        source="abc_color",
        source_url="https://www.abc.com.py/pf/api/v3/content/fetch/sections-api",
        created_at=datetime(2026, 1, 2, 10, 0, 0),
        request_url="https://www.abc.com.py/pf/api/v3/content/fetch/sections-api",
        request_params={},
        status_code=200,
        response_headers={},
        body=compress(json.dumps({"content_elements": []})),
    )

    assert parser.process_record(record) == []
