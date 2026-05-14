from datetime import datetime
from decimal import Decimal

from galactus.transform.html_parser import compress
from galactus.transform.parsers.supermercados.casarica import Parser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from tests.unit.fakes import make_parser

PRODUCT_HTML = """
<!DOCTYPE html>
<html>
  <body>
    <header><nav>Inicio</nav></header>
    <main>
      <div class="detalle">
        <h2>LECHE LA SERENISIMA ENTERA UAT 1LT</h2>
        <span class="precio">₲. 10.600</span>
        <span class="codigo">CÓDIGO: 7790742363008</span>
      </div>
    </main>
  </body>
</html>
"""

NO_H2_HTML = "<html><body><h1>Categoría</h1><p>nope</p></body></html>"


def _snapshot(html: str, url: str) -> HtmlSnapshot:
    return HtmlSnapshot(
        bronze_id=1,
        source="casarica",
        source_url=url,
        created_at=datetime(2026, 5, 14, 10, 0, 0),
        status_code=200,
        content_type="text/html",
        response_headers={},
        html=compress(html),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="casarica")


def test_build_entities_extracts_product() -> None:
    parser = _parser()
    record = _snapshot(
        PRODUCT_HTML,
        "https://www.casarica.com.py/leche-la-serenisima-entera-uat-1lt-p16949",
    )

    products = parser.build_entities(record, parser.decode(record))

    assert len(products) == 1
    product = products[0]
    assert isinstance(product, Product)
    assert product.source == "casarica"
    assert product.source_url == record.source_url
    assert product.name == "LECHE LA SERENISIMA ENTERA UAT 1LT"
    assert product.sku == "7790742363008"
    assert product.price == Decimal("10600")
    assert product.currency == "PYG"
    assert product.image_urls == [
        "https://casarica.cdn1.dattamax.com/userfiles/images/productos/600/7790742363008.jpg"
    ]


def test_page_without_h2_is_skipped() -> None:
    parser = _parser()
    record = _snapshot(NO_H2_HTML, "https://www.casarica.com.py/categoria-c1")

    assert parser.build_entities(record, parser.decode(record)) == []
