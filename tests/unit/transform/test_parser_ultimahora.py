from datetime import datetime

from galactus.transform.html_parser import compress
from galactus.transform.parsers.noticias.ultimahora import Parser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.article import Article
from tests.unit.fakes import make_parser

ARTICLE_HTML = """
<!DOCTYPE html>
<html>
  <head>
    <meta property="og:title" content="OG fallback title" />
    <meta property="og:image" content="https://www.ultimahora.com/img/hero-og.jpg" />
    <meta property="article:section" content="Política" />
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "NewsArticle",
      "headline": "Gobierno anuncia nuevo plan económico",
      "datePublished": "2026-01-02T10:30:00-03:00",
      "author": {"@type": "Person", "name": "Juana Pérez"},
      "image": [{"@type": "ImageObject", "url": "https://www.ultimahora.com/img/hero.jpg"}]
    }
    </script>
  </head>
  <body>
    <nav class="Breadcrumb"><a href="/economia">Economía</a></nav>
    <div class="RichTextArticleBody">
      <p>Primer párrafo del cuerpo de la nota.</p>
      <p>Segundo párrafo con más detalle.</p>
      <p>   </p>
      <img src="https://www.ultimahora.com/img/body-1.jpg" />
      <img src="https://www.ultimahora.com/img/logo.png" />
    </div>
  </body>
</html>
"""

EMPTY_HTML = "<html><head></head><body><p>just some text</p></body></html>"


def _snapshot(html: str) -> HtmlSnapshot:
    return HtmlSnapshot(
        bronze_id=1,
        source="ultimahora",
        source_url="https://www.ultimahora.com/gobierno-anuncia-nuevo-plan-economico",
        created_at=datetime(2026, 1, 2, 10, 0, 0),
        status_code=200,
        content_type="text/html",
        response_headers={},
        html=compress(html),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="ultimahora")


def test_build_entities_from_json_ld() -> None:
    parser = _parser()
    record = _snapshot(ARTICLE_HTML)

    articles = parser.process_record(record)

    assert len(articles) == 1
    article = articles[0]
    assert isinstance(article, Article)
    assert article.source == "ultimahora"
    assert article.source_url == record.source_url
    assert article.title == "Gobierno anuncia nuevo plan económico"
    assert article.authors == ["Juana Pérez"]
    assert isinstance(article.published_at, datetime)
    assert article.published_at.year == 2026
    # article:section meta wins over the breadcrumb
    assert article.section == "Política"
    assert article.body == (
        "Primer párrafo del cuerpo de la nota.\n\nSegundo párrafo con más detalle."
    )
    # hero image first, body image next, the logo filtered out
    assert article.image_urls == [
        "https://www.ultimahora.com/img/hero.jpg",
        "https://www.ultimahora.com/img/body-1.jpg",
    ]


def test_section_falls_back_to_breadcrumb() -> None:
    parser = _parser()
    html = ARTICLE_HTML.replace('<meta property="article:section" content="Política" />', "")
    record = _snapshot(html)

    article = parser.process_record(record)[0]

    assert article.section == "Economía"


def test_page_without_title_yields_empty_titled_article() -> None:
    parser = _parser()
    record = _snapshot(EMPTY_HTML)

    articles = parser.process_record(record)

    assert len(articles) == 1
    assert articles[0].title == ""
