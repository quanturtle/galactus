import json
from datetime import datetime
from decimal import Decimal

from galactus.transform.html_parser import compress
from galactus.transform.parsers.supermercados.biggie import Parser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.product import Product
from tests.unit.fakes import make_parser

RESPONSE = {
    "items": [
        {
            "name": "Yerba Mate Selecta 1kg",
            "code": "12345",
            "brand": {"name": "Selecta"},
            "family": {"name": "Yerba Mate", "classification": {"name": "Almacén"}},
            "price": 18000,
            "isOnOffer": True,
            "priceSaleOffer": 15990,
            "images": [
                {"type": 0, "src": "https://biggie.com.py/img/12345-full.jpg"},
                {"type": 1, "src": "https://biggie.com.py/img/12345-thumb.jpg"},
            ],
        },
        {
            "name": "Gaseosa Cola 2L",
            "code": "67890",
            "price": 9500,
            "images": [],
        },
        {"name": "", "code": "00000", "price": 100},
    ]
}


def _snapshot() -> ApiSnapshot:
    return ApiSnapshot(
        bronze_id=1,
        source="biggie",
        source_url="https://api.app.biggie.com.py/api/articles",
        created_at=datetime(2026, 1, 2, 10, 0, 0),
        request_url="https://api.app.biggie.com.py/api/articles?take=100&skip=0",
        request_params={"take": 100, "skip": 0},
        status_code=200,
        response_headers={},
        body=compress(json.dumps(RESPONSE)),
    )


def _parser() -> Parser:
    return make_parser(Parser, source="biggie")


def test_process_record_maps_items() -> None:
    parser = _parser()
    record = _snapshot()

    products = parser.process_record(record)

    # all entries become a Product, even the name-less one
    assert len(products) == 3
    first = products[0]
    assert isinstance(first, Product)
    assert first.source == "biggie"
    assert first.name == "Yerba Mate Selecta 1kg"
    assert first.brand == "Selecta"
    assert first.sku == "12345"
    # offer price wins; currency stays at the model default
    assert first.price == Decimal(15990)
    assert first.currency == "PYG"
    # only the type-0 image is kept
    assert first.image_urls == ["https://biggie.com.py/img/12345-full.jpg"]
    # source_url is rebuilt from the slugified name + code
    assert first.source_url == "https://biggie.com.py/item/yerba-mate-selecta-1kg-12345"

    second = products[1]
    assert second.name == "Gaseosa Cola 2L"
    assert second.brand is None
    assert second.price == Decimal(9500)
    assert second.image_urls == []
    assert second.source_url == "https://biggie.com.py/item/gaseosa-cola-2l-67890"

    third = products[2]
    assert third.name == ""
    assert third.sku == "00000"
