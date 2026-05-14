from datetime import datetime

from galactus.transform.html_parser import compress
from galactus.transform.parsers.noticias.npy import Parser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.article import Article
from tests.unit.fakes import make_parser

ARTICLE_HTML = """
<!DOCTYPE html>
<html>
  <head><title>Tres selecciones oficializan listas | NPY</title></head>
  <body>
    <header><nav>Inicio</nav></header>
    <main>
      <article>
        <h1>Tres selecciones oficializan las primeras listas para el Mundial 2026</h1>
        <div class="meta">Publicado Mayo 14, 2026 11:20 a. m.</div>
        <p>Primer párrafo de la nota deportiva.</p>
        <p>Segundo párrafo con detalle de las convocatorias.</p>
        <img src="https://grupovierci.brightspotcdn.com/abc/123.jpg" />
        <img src="https://npy.com.py/static/logo-share.png" />
      </article>
    </main>
    <footer>© NPY 2026</footer>
  </body>
</html>
"""

PM_HTML = ARTICLE_HTML.replace("11:20 a. m.", "08:45 p. m.")
EMPTY_HTML = "<html><head></head><body><p>nope</p></body></html>"


def _snapshot(html: str, url: str) -> HtmlSnapshot:
    return HtmlSnapshot(
        bronze_id=1,
        source="npy",
        source_url=url,
        created_at=datetime(2026, 5, 14, 11, 20, 0),
        status_code=200,
        content_type="text/html",
        response_headers={},
        html=compress(html),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="npy")


def test_build_entities_extracts_fields() -> None:
    parser = _parser()
    record = _snapshot(
        ARTICLE_HTML,
        "https://www.npy.com.py/noticias/deportes/tres-selecciones-oficializan-las-primeras-listas",
    )

    articles = parser.build_entities(record, parser.decode(record))

    assert len(articles) == 1
    article = articles[0]
    assert isinstance(article, Article)
    assert article.source == "npy"
    assert article.source_url == record.source_url
    assert article.title == (
        "Tres selecciones oficializan las primeras listas para el Mundial 2026"
    )
    assert article.body == (
        "Primer párrafo de la nota deportiva.\n\n"
        "Segundo párrafo con detalle de las convocatorias."
    )
    # section comes from the URL
    assert article.section == "Deportes"
    # logo filtered out
    assert article.image_urls == ["https://grupovierci.brightspotcdn.com/abc/123.jpg"]
    assert article.published_at == datetime(2026, 5, 14, 11, 20)


def test_pm_meridiem_converts_to_24h() -> None:
    parser = _parser()
    record = _snapshot(
        PM_HTML,
        "https://www.npy.com.py/noticias/deportes/foo",
    )

    article = parser.build_entities(record, parser.decode(record))[0]

    assert article.published_at == datetime(2026, 5, 14, 20, 45)


def test_page_without_title_is_skipped() -> None:
    parser = _parser()
    record = _snapshot(EMPTY_HTML, "https://www.npy.com.py/noticias/deportes/x")

    assert parser.build_entities(record, parser.decode(record)) == []
