import json
from datetime import datetime

from galactus.infra.db import Database
from galactus.transform.parsers.noticias.lanacion import Parser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.article import Article
from tests.unit.fakes import make_parser

RESPONSE = {
    "content_elements": [
        {
            "_id": "LN-1",
            "type": "story",
            "canonical_url": "/politica/2026/04/05/nota-de-prueba/",
            "headlines": {"basic": "Nota de prueba en La Nación"},
            "publish_date": "2026-04-05T13:15:00Z",
            "credits": {"by": [{"name": "Ana Báez", "type": "author"}]},
            "taxonomy": {
                "primary_section": {"name": "Política"},
                "tags": [{"text": "Congreso"}],
            },
            "promo_items": {
                "basic": {"type": "image", "url": "https://www.lanacion.com.py/img/p.jpg"}
            },
            "content_elements": [
                {"type": "text", "content": "<p>Cuerpo principal.</p>"},
                {"type": "text", "content": "Cierre."},
            ],
        }
    ]
}


def _snapshot() -> ApiSnapshot:
    return ApiSnapshot(
        id=1,
        source="lanacion",
        source_url="https://www.lanacion.com.py/pf/api/v3/content/fetch/content-search-feed-full",
        created_at=datetime(2026, 4, 5, 13, 0, 0),
        request_url="https://www.lanacion.com.py/pf/api/v3/content/fetch/content-search-feed-full",
        request_params={"feedSize": "100"},
        status_code=200,
        response_headers={},
        body=Database.compress(json.dumps(RESPONSE)),
    )


def test_build_entities_maps_stories() -> None:
    parser = make_parser(Parser, source="lanacion")
    record = _snapshot()

    articles = parser.process_record(record)

    assert len(articles) == 1
    article = articles[0]
    assert isinstance(article, Article)
    assert article.source == "lanacion"
    assert article.source_url == ("https://www.lanacion.com.py/politica/2026/04/05/nota-de-prueba/")
    assert article.title == "Nota de prueba en La Nación"
    assert article.body == "Cuerpo principal.\n\nCierre."
    assert article.authors == ["Ana Báez"]
    assert article.section == "Política"
    assert article.tags == ["Congreso"]
    assert article.image_urls == ["https://www.lanacion.com.py/img/p.jpg"]
    assert isinstance(article.published_at, datetime)
    assert article.published_at.year == 2026
