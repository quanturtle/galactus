from datetime import datetime

from galactus.transform.parsers.supermercados.stock import Parser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from tests.unit.fakes import FakeDatabase, make_parser


def _snapshot(html: str) -> HtmlSnapshot:
    return HtmlSnapshot(
        id=1,
        source="stock",
        source_url="https://www.stock.com.py/products/139380-desodorante-rexona-men-50gr.aspx",
        created_at=datetime(2026, 5, 14, 10, 0, 0),
        status_code=200,
        content_type="text/html",
        response_headers={},
        html=FakeDatabase().compress(html),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="stock")


def test_brand_prefers_json_ld_over_manufacturers_link() -> None:
    parser = _parser()
    html = """
    <!DOCTYPE html>
    <html>
      <head>
        <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"Product",
           "name":"Desodorante Invisible Dry Rexona Men 50gr",
           "brand":{"@type":"Brand","name":"Rexona"}}
        </script>
      </head>
      <body>
        <div class="manufacturers"><a href="/manufacturer/dove">DOVE</a></div>
      </body>
    </html>
    """
    item = parser.decode(_snapshot(html))

    # JSON-LD is canonical; Stock's .manufacturers a is wrong on this PDP
    assert parser.extract_brand(item) == "Rexona"


def test_brand_falls_back_to_manufacturers_link_when_no_json_ld() -> None:
    parser = _parser()
    html = """
    <!DOCTYPE html>
    <html>
      <body>
        <div class="manufacturers"><a href="/manufacturer/colgate">COLGATE</a></div>
      </body>
    </html>
    """
    item = parser.decode(_snapshot(html))

    assert parser.extract_brand(item) == "COLGATE"


def test_brand_is_none_when_neither_source_present() -> None:
    parser = _parser()
    html = "<html><body><h1>Producto sin marca</h1></body></html>"
    item = parser.decode(_snapshot(html))

    assert parser.extract_brand(item) is None


def test_brand_handles_string_brand_in_json_ld() -> None:
    parser = _parser()
    html = """
    <!DOCTYPE html>
    <html>
      <head>
        <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"Product",
           "name":"Crema dental Colgate",
           "brand":"Colgate"}
        </script>
      </head>
      <body></body>
    </html>
    """
    item = parser.decode(_snapshot(html))

    assert parser.extract_brand(item) == "Colgate"
