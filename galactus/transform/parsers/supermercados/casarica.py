import re
from decimal import Decimal

from galactus.transform.base_parser import BaseParser
from galactus.transform.product_parser import ProductParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from sql.base import Base

# "₲. 10.600" / "₲ 10.600" → "10.600" (Paraguayan thousands separator is ".")
PRICE_PATTERN = re.compile(r"₲\.?\s*([\d.]+)")

# "CÓDIGO: 7790742363008" / "CODIGO: 7790742363008" → "7790742363008"
SKU_PATTERN = re.compile(r"C[ÓO]DIGO\s*:\s*(\w+)", re.IGNORECASE)

CDN_IMAGE_TEMPLATE = "https://casarica.cdn1.dattamax.com/userfiles/images/productos/600/{sku}.jpg"


class Parser(BaseParser, ProductParser):
    """Parses HtmlSnapshots from casarica.com.py into Product entities.

    casarica runs on the Dattamax SaaS platform (the same backend as
    arete): no JSON-LD, no OpenGraph. Name comes from
    ``<h1 class="product_title">``; price and sku come from regex against
    visible text ("₲. 10.600", "CÓDIGO: 7790742363008"); the image is
    reconstructed from the SKU on Dattamax's CDN.

    decode() wraps the parsed soup together with the bronze source_url
    so the eight extract_* hooks need nothing else. One Product per
    bronze record, so build_item is inherited (returns [decoded]).
    """

    bronze_model = HtmlSnapshot
    silver_model = Product

    CURRENCY = "PYG"

    def decode(self, record: Base) -> dict:
        return {"soup": super().decode(record), "source_url": record.source_url}

    def extract_source_url(self, item: dict) -> str:
        return item["source_url"]

    # "CÓDIGO: 7790742363008" -> "7790742363008"
    def extract_sku(self, item: dict) -> str | None:
        match = SKU_PATTERN.search(item["soup"].get_text(" ", strip=True))
        if not match:
            return None
        return match.group(1).strip() or None

    def extract_name(self, item: dict) -> str:
        h1 = item["soup"].select_one("h1.product_title")
        return h1.get_text(" ", strip=True) if h1 else ""

    def extract_brand(self, item: dict) -> str | None:
        return None

    # "₲. 10.600" -> Decimal("10600")
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

    # image is reconstructed from the sku on Dattamax's CDN; absent without one
    def extract_image_urls(self, item: dict) -> list[str]:
        sku = self.extract_sku(item)
        if not sku:
            return []
        return [CDN_IMAGE_TEMPLATE.format(sku=sku)]
