import re
from decimal import Decimal
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from sql.base import Base

# "₲. 24.000" / "₲ 24.000" → "24.000" (Paraguayan thousands separator is ".")
PRICE_PATTERN = re.compile(r"₲\.?\s*([\d.]+)")

# "CÓDIGO: 72000" / "CODIGO: 72000" → "72000"
SKU_PATTERN = re.compile(r"C[ÓO]DIGO\s*:\s*(\w+)", re.IGNORECASE)


class Parser(BaseParser):
    """Parses HtmlSnapshots from arete.com.py into Product entities.

    arete runs on the Dattamax SaaS platform: no JSON-LD, no OpenGraph,
    no semantic class names. Name comes from ``<h1>``; the price and the
    SKU are extracted by regex from visible text ("₲. 24.000" and "CÓDIGO:
    72000"); the hero image is reconstructed from the SKU
    (``/userfiles/images/productos/<sku>.jpg``) since no inline product
    gallery selector is exposed. Pages without a name are skipped.
    """

    bronze_model = HtmlSnapshot
    silver_model = Product

    SITE_BASE = "https://www.arete.com.py"
    PRODUCT_IMAGE_PATH = "/userfiles/images/productos/{sku}.jpg"
    CURRENCY = "PYG"

    # "Gs   195.000" / "₲. 195.000" -> Decimal("195000")
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

    # "CÓDIGO: 72000" -> "72000"
    def _parse_sku(self, text: str) -> str | None:
        match = SKU_PATTERN.search(text)
        if not match:
            return None
        return match.group(1).strip() or None

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        soup: BeautifulSoup = decoded

        # name is required; skip non-product pages outright
        h1 = soup.find("h1")
        if h1 is None:
            return []
        name = h1.get_text(" ", strip=True)
        if not name:
            return []

        page_text = soup.get_text(" ", strip=True)
        price = self._parse_price(page_text)
        sku = self._parse_sku(page_text)

        # image is reconstructed from the sku; absent without one
        image_urls: list[str] = []
        if sku:
            image_urls.append(urljoin(self.SITE_BASE, self.PRODUCT_IMAGE_PATH.format(sku=sku)))

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
