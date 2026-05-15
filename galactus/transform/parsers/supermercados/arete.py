import re
from decimal import Decimal
from urllib.parse import urljoin

from galactus.transform.base_parser import BaseParser
from galactus.transform.product_parser import ProductParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from sql.base import Base

# "₲. 24.000" / "₲ 24.000" → "24.000" (Paraguayan thousands separator is ".")
PRICE_PATTERN = re.compile(r"₲\.?\s*([\d.]+)")

# "CÓDIGO: 72000" / "CODIGO: 72000" → "72000"
SKU_PATTERN = re.compile(r"C[ÓO]DIGO\s*:\s*(\w+)", re.IGNORECASE)


class Parser(BaseParser, ProductParser):
    """Parses HtmlSnapshots from arete.com.py into Product entities.

    arete runs on the Dattamax SaaS platform: no JSON-LD, no OpenGraph,
    no semantic class names. Name comes from ``<h1>``; the price and the
    SKU are extracted by regex from visible text ("₲. 24.000" and "CÓDIGO:
    72000"); the hero image is reconstructed from the SKU
    (``/userfiles/images/productos/<sku>.jpg``) since no inline product
    gallery selector is exposed.

    decode() wraps the parsed soup together with the bronze source_url
    so the eight extract_* hooks need nothing else. One Product per
    bronze record, so build_item is inherited (returns [decoded]).
    """

    bronze_model = HtmlSnapshot
    silver_model = Product

    SITE_BASE = "https://www.arete.com.py"
    PRODUCT_IMAGE_PATH = "/userfiles/images/productos/{sku}.jpg"
    CURRENCY = "PYG"

    def decode(self, record: Base) -> dict:
        return {"soup": super().decode(record), "source_url": record.source_url}

    def extract_source_url(self, item: dict) -> str:
        return item["source_url"]

    # "CÓDIGO: 72000" -> "72000"
    def extract_sku(self, item: dict) -> str | None:
        match = SKU_PATTERN.search(item["soup"].get_text(" ", strip=True))
        if not match:
            return None
        return match.group(1).strip() or None

    def extract_name(self, item: dict) -> str:
        h1 = item["soup"].find("h1")
        return h1.get_text(" ", strip=True) if h1 else ""

    def extract_brand(self, item: dict) -> str | None:
        return None

    # "Gs   195.000" / "₲. 195.000" -> Decimal("195000")
    def extract_price(self, item: dict) -> Decimal | None:
        match = PRICE_PATTERN.search(item["soup"].get_text(" ", strip=True))
        if not match:
            return None
        digits = match.group(1).replace(".", "")
        if not digits:
            return None
        try:
            return Decimal(digits)
        except (ValueError, ArithmeticError):
            return None

    def extract_currency(self, item: dict) -> str:
        return self.CURRENCY

    def extract_unit(self, item: dict) -> str | None:
        return self.parse_unit_from_name(self.extract_name(item))

    # image is reconstructed from the sku; absent without one
    def extract_image_urls(self, item: dict) -> list[str]:
        sku = self.extract_sku(item)
        if not sku:
            return []
        return [urljoin(self.SITE_BASE, self.PRODUCT_IMAGE_PATH.format(sku=sku))]
