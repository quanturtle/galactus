from datetime import datetime
from decimal import Decimal

from galactus.transform.html_parser import compress
from galactus.transform.parsers.supermercados.superseis import Parser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from tests.unit.fakes import make_parser

PRODUCT_HTML = """
<!DOCTYPE html>
<html>
  <head>
    <meta property="og:image" content="https://cdn.sd.com.py/ocs6/.../og.jpg" />
    <script type="application/ld+json">
      {"@context":"https://schema.org","@type":"OnlineStore","name":"Superseis Online"}
    </script>
    <script type="application/ld+json">
      {"@context":"https://schema.org","@type":"Product",
       "name":"Entraña gruesa por kilo",
       "sku":"378956",
       "brand":{"@type":"Brand","name":"TRAZABEEF"},
       "offers":{"@type":"Offer","priceCurrency":"PYG","price":"39950"},
       "image":"https://cdn.sd.com.py/ocs6/.../product.jpg"}
    </script>
  </head>
  <body><h1>Entraña gruesa por kilo</h1></body>
</html>
"""

NO_PRODUCT_HTML = """
<!DOCTYPE html>
<html>
  <head>
    <script type="application/ld+json">
      {"@context":"https://schema.org","@type":"OnlineStore","name":"Superseis Online"}
    </script>
  </head>
  <body><div>Category page</div></body>
</html>
"""

OG_FALLBACK_HTML = """
<!DOCTYPE html>
<html>
  <head>
    <meta property="og:image" content="https://cdn.sd.com.py/ocs6/.../og-fallback.jpg" />
    <script type="application/ld+json">
      {"@context":"https://schema.org","@type":"Product",
       "name":"Producto sin imagen JSON-LD",
       "sku":"X1",
       "offers":{"price":"1000","priceCurrency":"PYG"}}
    </script>
  </head>
  <body></body>
</html>
"""


def _snapshot(html: str, url: str) -> HtmlSnapshot:
    return HtmlSnapshot(
        bronze_id=1,
        source="superseis",
        source_url=url,
        created_at=datetime(2026, 5, 14, 10, 0, 0),
        status_code=200,
        content_type="text/html",
        response_headers={},
        html=compress(html),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="superseis")


def test_process_record_from_json_ld() -> None:
    parser = _parser()
    record = _snapshot(
        PRODUCT_HTML,
        "https://www.superseis.com.py/product/entrana-gruesa-por-kilo",
    )

    products = parser.process_record(record)

    assert len(products) == 1
    product = products[0]
    assert isinstance(product, Product)
    assert product.source == "superseis"
    assert product.source_url == record.source_url
    assert product.name == "Entraña gruesa por kilo"
    assert product.sku == "378956"
    assert product.brand == "TRAZABEEF"
    assert product.price == Decimal("39950")
    assert product.currency == "PYG"
    # JSON-LD image wins over og:image
    assert product.image_urls == ["https://cdn.sd.com.py/ocs6/.../product.jpg"]


def test_page_without_product_jsonld_yields_empty_named_product() -> None:
    parser = _parser()
    record = _snapshot(NO_PRODUCT_HTML, "https://www.superseis.com.py/catalog/something")

    products = parser.process_record(record)

    assert len(products) == 1
    assert products[0].name == ""
    assert products[0].sku is None
    assert products[0].price is None


def test_image_falls_back_to_og() -> None:
    parser = _parser()
    record = _snapshot(OG_FALLBACK_HTML, "https://www.superseis.com.py/product/x")

    product = parser.process_record(record)[0]

    assert product.image_urls == ["https://cdn.sd.com.py/ocs6/.../og-fallback.jpg"]
