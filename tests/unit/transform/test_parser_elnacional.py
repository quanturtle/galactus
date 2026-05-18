from datetime import datetime

from galactus.infra.db import Database
from galactus.transform.parsers.noticias.elnacional import Parser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.article import Article
from tests.unit.fakes import make_parser

ARTICLE_HTML = """
<!DOCTYPE html>
<html>
  <head>
    <meta property="og:title" content="OG fallback title" />
    <meta property="og:image" content="https://elnacional.com.py/img/hero-og.webp" />
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "NewsArticle",
      "headline": "Defensa de funcionaria afirma que nunca estuvo prófuga",
      "datePublished": "2026-05-14T11:20:00-03:00",
      "articleSection": "Política",
      "keywords": ["Justicia", "Ministerio Público"],
      "author": [{"@type": "Organization", "name": "El Nacional"}],
      "image": [{"@type": "ImageObject", "url": "https://elnacional.com.py/img/hero.jpg"}]
    }
    </script>
  </head>
  <body>
    <article>
      <div class="content">
        <p>Primer párrafo de la nota.</p>
        <p>Segundo párrafo, con más detalle.</p>
        <p>   </p>
        <img src="https://elnacional.com.py/img/cuerpo-1.jpg" />
        <img src="https://elnacional.com.py/img/icon-share.png" />
      </div>
    </article>
  </body>
</html>
"""

EMPTY_HTML = "<html><head></head><body><p>just some text</p></body></html>"


def _snapshot(html: str) -> HtmlSnapshot:
    return HtmlSnapshot(
        id=1,
        source="elnacional",
        source_url=(
            "https://elnacional.com.py/politica/defensa-funcionaria-afirma-nunca-estuvo-profuga-n105824"
        ),
        created_at=datetime(2026, 5, 14, 11, 20, 0),
        status_code=200,
        content_type="text/html",
        response_headers={},
        html=Database.compress(html),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="elnacional")


def test_build_entities_from_json_ld() -> None:
    parser = _parser()
    record = _snapshot(ARTICLE_HTML)

    articles = parser.process_record(record)

    assert len(articles) == 1
    article = articles[0]
    assert isinstance(article, Article)
    assert article.source == "elnacional"
    assert article.source_url == record.source_url
    assert article.title == "Defensa de funcionaria afirma que nunca estuvo prófuga"
    assert article.authors == ["El Nacional"]
    assert article.section == "Política"
    assert article.tags == ["Justicia", "Ministerio Público"]
    assert article.body == ("Primer párrafo de la nota.\n\nSegundo párrafo, con más detalle.")
    # hero image first, body image next, the icon filtered out
    assert article.image_urls == [
        "https://elnacional.com.py/img/hero.jpg",
        "https://elnacional.com.py/img/cuerpo-1.jpg",
    ]
    assert isinstance(article.published_at, datetime)
    assert article.published_at.year == 2026
    assert article.published_at.month == 5


def test_page_without_title_yields_empty_titled_article() -> None:
    parser = _parser()
    record = _snapshot(EMPTY_HTML)

    articles = parser.process_record(record)

    assert len(articles) == 1
    assert articles[0].title == ""
