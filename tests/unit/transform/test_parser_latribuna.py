import json
from datetime import datetime

from galactus.transform.parsers.noticias.latribuna import Parser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.article import Article
from tests.unit.fakes import FakeDatabase, make_parser

RESPONSE = {
    "content_elements": [
        {
            "_id": "LT-1",
            "type": "story",
            "canonical_url": "/economia/2026/04/06/cierre-bursatil/",
            "headlines": {"basic": "Cierre bursátil"},
            "first_publish_date": "2026-04-06T22:05:00Z",
            "credits": {"by": [{"name": "Carlos Insfrán"}]},
            "taxonomy": {
                "sections": [{"name": "Economía"}],
                "tags": [{"text": "Mercados"}],
            },
            "content_elements": [{"type": "text", "content": "<p>Resumen de la jornada.</p>"}],
        },
        {"type": "story", "headlines": {"basic": ""}},
    ]
}


def test_build_entities_maps_stories() -> None:
    parser = make_parser(Parser, source="latribuna")
    record = ApiSnapshot(
        id=1,
        source="latribuna",
        source_url="https://www.latribuna.com.py/pf/api/v3/content/fetch/story-feed-query",
        created_at=datetime(2026, 4, 6, 22, 0, 0),
        request_url="https://www.latribuna.com.py/pf/api/v3/content/fetch/story-feed-query",
        request_params={"size": "100"},
        status_code=200,
        response_headers={},
        body=FakeDatabase().compress(json.dumps(RESPONSE)),
    )

    articles = parser.process_record(record)

    # every story becomes an Article, even the headline-less one
    assert len(articles) == 2
    article = articles[0]
    assert isinstance(article, Article)
    assert article.source == "latribuna"
    assert article.source_url == (
        "https://www.latribuna.com.py/economia/2026/04/06/cierre-bursatil/"
    )
    assert article.title == "Cierre bursátil"
    assert article.body == "Resumen de la jornada."
    assert article.authors == ["Carlos Insfrán"]
    # falls back to sections[0].name when primary_section is missing
    assert article.section == "Economía"
    assert article.tags == ["Mercados"]
    # no promo_items, no inner images
    assert article.image_urls == []
    assert isinstance(article.published_at, datetime)
    assert article.published_at.year == 2026
