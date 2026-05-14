import re
from decimal import Decimal
from typing import Any

from bs4 import BeautifulSoup

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from sql.base import Base

# "₲. 10.600" / "₲ 10.600" → "10.600" (Paraguayan thousands separator is ".")
PRICE_PATTERN = re.compile(r"₲\.?\s*([\d.]+)")

# "CÓDIGO: 7790742363008" / "CODIGO: 7790742363008" → "7790742363008"
SKU_PATTERN = re.compile(r"C[ÓO]DIGO\s*:\s*(\w+)", re.IGNORECASE)

CDN_IMAGE_TEMPLATE = "https://casarica.cdn1.dattamax.com/userfiles/images/productos/600/{sku}.jpg"


class Parser(BaseParser):
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

    # "CÓDIGO: 7790742363008" -> "7790742363008"
    def _parse_sku(self, text: str) -> str | None:
        match = SKU_PATTERN.search(text)
        if not match:
            return None
        return match.group(1).strip() or None

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        soup: BeautifulSoup = decoded

        # name lives in the product detail's h2; skip pages without one
        h2 = soup.find("h2")
        if h2 is None:
            return []
        name = h2.get_text(" ", strip=True)
        if not name:
            return []

        page_text = soup.get_text(" ", strip=True)
        price = self._parse_price(page_text)
        sku = self._parse_sku(page_text)

        # image is reconstructed from the sku on Dattamax's CDN; absent without one
        image_urls: list[str] = []
        if sku:
            image_urls.append(CDN_IMAGE_TEMPLATE.format(sku=sku))

        return [
            Product(
                source=self.source,
                source_url=record.source_url,
                name=name,
                sku=sku,
                price=price,
                currency=self.CURRENCY,
                image_urls=image_urls,
            )
        ]
