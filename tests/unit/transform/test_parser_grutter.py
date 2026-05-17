import json
from datetime import datetime
from decimal import Decimal

from galactus.transform.html_parser import compress
from galactus.transform.parsers.supermercados.grutter import Parser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.product import Product
from tests.unit.fakes import make_parser

RESPONSE = [
    {
        "id": 101,
        "name": "Aceite Cocinero 1L",
        "sku": "ACE001",
        "permalink": "https://grutteronline.casagrutter.com.py/producto/aceite-cocinero-1l/",
        "prices": {
            "currency_code": "PYG",
            "currency_minor_unit": 0,
            "price": "12500",
            "regular_price": "13000",
            "sale_price": "12500",
        },
        "images": [
            {"src": "https://grutter.com.py/img/ace001-1.jpg"},
            {"src": "https://grutter.com.py/img/ace001-1.jpg"},
            {"src": "https://grutter.com.py/img/ace001-2.jpg"},
        ],
    },
    {
        "id": 102,
        "name": "Yogurt USD-priced edge",
        "sku": "USD-1",
        "permalink": "https://grutteronline.casagrutter.com.py/producto/yogurt/",
        "prices": {
            "currency_code": "USD",
            "currency_minor_unit": 2,
            "price": "199",
        },
        "images": [],
    },
    {"id": 103, "name": "", "sku": "SKIP", "prices": {}},
]


def _snapshot() -> ApiSnapshot:
    return ApiSnapshot(
        id=1,
        source="grutter",
        source_url="https://grutteronline.casagrutter.com.py/wp-json/wc/store/v1/products?page=1",
        created_at=datetime(2026, 5, 14, 10, 0, 0),
        request_url="https://grutteronline.casagrutter.com.py/wp-json/wc/store/v1/products?page=1",
        request_params={"page": "1", "per_page": "100"},
        status_code=200,
        response_headers={},
        body=compress(json.dumps(RESPONSE)),
    )


def test_process_record_maps_products() -> None:
    parser = make_parser(Parser, source="grutter")
    record = _snapshot()

    products = parser.process_record(record)

    # all entries become a Product, even the name-less one
    assert len(products) == 3

    first = products[0]
    assert isinstance(first, Product)
    assert first.source == "grutter"
    assert first.source_url == (
        "https://grutteronline.casagrutter.com.py/producto/aceite-cocinero-1l/"
    )
    assert first.name == "Aceite Cocinero 1L"
    assert first.sku == "ACE001"
    assert first.price == Decimal("12500")
    assert first.currency == "PYG"
    # images deduped, order preserved
    assert first.image_urls == [
        "https://grutter.com.py/img/ace001-1.jpg",
        "https://grutter.com.py/img/ace001-2.jpg",
    ]

    # USD with currency_minor_unit=2 — 199 → 1.99
    second = products[1]
    assert second.currency == "USD"
    assert second.price == Decimal("1.99")
    assert second.image_urls == []

    third = products[2]
    assert third.name == ""
    assert third.sku == "SKIP"
