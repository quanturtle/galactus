from datetime import datetime
from decimal import Decimal

from galactus.transform.html_parser import compress
from galactus.transform.parsers.supermercados.arete import Parser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from tests.unit.fakes import make_parser

PRODUCT_HTML = """
<!DOCTYPE html>
<html>
  <body>
    <header><nav>Inicio</nav></header>
    <main>
      <h1>CARIMBATA X KG.</h1>
      <div class="precio">₲. 24.000</div>
      <div class="codigo">CÓDIGO: 72000</div>
      <p>Descripción del producto.</p>
    </main>
  </body>
</html>
"""

NO_H1_HTML = "<html><body><div>not a product page</div></body></html>"


def _snapshot(html: str, url: str) -> HtmlSnapshot:
    return HtmlSnapshot(
        bronze_id=1,
        source="arete",
        source_url=url,
        created_at=datetime(2026, 5, 14, 10, 0, 0),
        status_code=200,
        content_type="text/html",
        response_headers={},
        html=compress(html),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="arete")


def test_process_record_extracts_product() -> None:
    parser = _parser()
    record = _snapshot(PRODUCT_HTML, "https://www.arete.com.py/carimbata-x-kg-p101166")

    products = parser.process_record(record)

    assert len(products) == 1
    product = products[0]
    assert isinstance(product, Product)
    assert product.source == "arete"
    assert product.source_url == record.source_url
    assert product.name == "CARIMBATA X KG."
    assert product.sku == "72000"
    assert product.price == Decimal("24000")
    assert product.currency == "PYG"
    assert product.image_urls == ["https://www.arete.com.py/userfiles/images/productos/72000.jpg"]


def test_page_without_h1_yields_empty_named_product() -> None:
    parser = _parser()
    record = _snapshot(NO_H1_HTML, "https://www.arete.com.py/categoria-c1")

    products = parser.process_record(record)

    assert len(products) == 1
    assert products[0].name == ""
    assert products[0].sku is None
    assert products[0].price is None
