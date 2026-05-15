import re
from decimal import Decimal
from typing import Any

from bs4 import BeautifulSoup

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

    casarica runs on the Dattamax SaaS platform (the same backend as arete)
    but its product detail block puts the name in ``<h2>`` instead of
    ``<h1>``. No JSON-LD, no OpenGraph: price and sku come from regex
    against visible text ("₲. 10.600", "CÓDIGO: 7790742363008") and the
    image is reconstructed from the SKU on Dattamax's CDN. Pages without a
    name are skipped.
    """

    bronze_model = HtmlSnapshot
    silver_model = Product

    CURRENCY = "PYG"

    def extract_source_url(self, item: BeautifulSoup, record: Base) -> str:
        return record.source_url

    # "CÓDIGO: 7790742363008" -> "7790742363008"
    def _parse_sku(self, text: str) -> str | None:
        match = SKU_PATTERN.search(text)
        if not match:
            return None
        return match.group(1).strip() or None

    def extract_sku(self, item: BeautifulSoup, record: Base) -> str | None:
        return self._parse_sku(item.get_text(" ", strip=True))

    # presence guaranteed by build_entities; re-selects to stay self-contained
    def extract_name(self, item: BeautifulSoup, record: Base) -> str:
        h2 = item.find("h2")
        return h2.get_text(" ", strip=True) if h2 else ""

    def extract_brand(self, item: BeautifulSoup, record: Base) -> str | None:
        return None

    # "₲. 10.600" -> Decimal("10600")
    def _parse_price(self, text: str) -> Decimal | None:
        match = PRICE_PATTERN.search(text)
        if not match:
            return None
        digits = match.group(1).replace(".", "")
        if not digits:
            return None
        try:
            return Decimal(digits)
        except (ValueError, ArithmeticError):
            return None

    def extract_price(self, item: BeautifulSoup, record: Base) -> Decimal | None:
        return self._parse_price(item.get_text(" ", strip=True))

    def extract_currency(self, item: BeautifulSoup, record: Base) -> str:
        return self.CURRENCY

    def extract_unit(self, item: BeautifulSoup, record: Base) -> str | None:
        return None

    # image is reconstructed from the sku on Dattamax's CDN; absent without one
    def extract_image_urls(self, item: BeautifulSoup, record: Base) -> list[str]:
        sku = self.extract_sku(item, record)
        if not sku:
            return []
        return [CDN_IMAGE_TEMPLATE.format(sku=sku)]

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        soup: BeautifulSoup = decoded
        # name lives in the product detail's h2; skip pages without one
        h2 = soup.find("h2")
        if h2 is None or not h2.get_text(" ", strip=True):
            return []
        return [self.build_entity(soup, record)]
