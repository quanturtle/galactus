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


def test_page_without_article_json_ld_is_skipped() -> None:
    parser = _parser()
    record = _snapshot(EMPTY_HTML)

    # pages without a NewsArticle/Article JSON-LD are hub/topic templates;
    # build_item drops them so they don't show up in silver as all-null rows
    assert parser.process_record(record) == []


def test_og_image_logo_falls_back_to_first_body_image() -> None:
    parser = _parser()
    html = """
    <!DOCTYPE html>
    <html>
      <head>
        <meta property="og:image" content="https://www.ultimahora.com/img/logo-completo.png" />
        <script type="application/ld+json">
        {
          "@context": "https://schema.org",
          "@type": "NewsArticle",
          "headline": "Editorial sin imagen en el JSON-LD",
          "datePublished": "2026-01-02T10:30:00-03:00",
          "author": {"@type": "Person", "name": "Editorial UH"}
        }
        </script>
      </head>
      <body>
        <div class="RichTextArticleBody">
          <p>Texto editorial.</p>
          <img src="https://www.ultimahora.com/img/real-hero.jpg" />
        </div>
      </body>
    </html>
    """
    record = _snapshot(html)

    article = parser.process_record(record)[0]

    # og:image is the publication logo, so it's rejected; the first body
    # image becomes the hero and is the only entry (no dedup against a logo)
    assert article.image_urls == ["https://www.ultimahora.com/img/real-hero.jpg"]
