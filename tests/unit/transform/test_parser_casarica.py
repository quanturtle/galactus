from datetime import datetime
from decimal import Decimal

from galactus.transform.parsers.supermercados.casarica import Parser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from tests.unit.fakes import FakeDatabase, make_parser

PRODUCT_HTML = """
<!DOCTYPE html>
<html>
  <body>
    <header><nav>Inicio</nav></header>
    <main>
      <div class="detalle">
        <h1 class="product_title">LECHE LA SERENISIMA ENTERA UAT 1LT</h1>
        <span class="precio">₲. 10.600</span>
        <span class="codigo">CÓDIGO: 7790742363008</span>
      </div>
    </main>
  </body>
</html>
"""

NO_PRODUCT_TITLE_HTML = "<html><body><h1>Categoría</h1><p>nope</p></body></html>"


def _snapshot(html: str, url: str) -> HtmlSnapshot:
    return HtmlSnapshot(
        id=1,
        source="casarica",
        source_url=url,
        created_at=datetime(2026, 5, 14, 10, 0, 0),
        status_code=200,
        content_type="text/html",
        response_headers={},
        html=FakeDatabase().compress(html),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="casarica")


def test_process_record_extracts_product() -> None:
    parser = _parser()
    record = _snapshot(
        PRODUCT_HTML,
        "https://www.casarica.com.py/leche-la-serenisima-entera-uat-1lt-p16949",
    )

    products = parser.process_record(record)

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


def test_page_without_product_title_yields_empty_named_product() -> None:
    parser = _parser()
    record = _snapshot(NO_PRODUCT_TITLE_HTML, "https://www.casarica.com.py/categoria-c1")

    products = parser.process_record(record)

    assert len(products) == 1
    assert products[0].name == ""
    assert products[0].sku is None
    assert products[0].price is None
