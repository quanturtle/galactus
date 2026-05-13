import re
from decimal import Decimal
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from sql.base import Base

_PRICE_DIGITS = re.compile(r"[\d.]+")


class Parser(BaseParser):
    """Parses HtmlSnapshots from stock.com.py into Product entities.

    Stock is a classic ASP.NET shop; each /products/ page exposes name,
    brand, price, and sku under stable itemprop / class hooks. Pages
    missing the name are skipped (silver.products requires it).
    """

    bronze_model = HtmlSnapshot
    silver_model = Product

    SITE_BASE = "https://www.stock.com.py"
    CURRENCY = "PYG"

    # "Gs   195.000" -> Decimal("195000"); "." is the Paraguayan thousands separator
    def _parse_price(self, text: str) -> Decimal | None:
        match = _PRICE_DIGITS.search(text)
        if not match:
            return None
        digits = match.group(0).replace(".", "")
        if not digits:
            return None
        try:
            return Decimal(digits)
        except ValueError:
            return None

    # "Código de Barras:8007150902781" -> "8007150902781"
    def _parse_sku(self, text: str) -> str | None:
        _, _, after = text.partition(":")
        sku = after.strip()
        return sku or None

    # absolutize + dedupe image srcs from the main product slider
    def _image_urls(self, soup: BeautifulSoup) -> list[str]:
        slider = soup.select_one("#img-slider .ubislider-inner")
        if slider is None:
            return []
        seen: set[str] = set()
        out: list[str] = []
        for img in slider.find_all("img"):
            src = (img.get("src") or "").strip()
            if not src:
                continue
            absolute = urljoin(self.SITE_BASE, src)
            if absolute in seen:
                continue
            seen.add(absolute)
            out.append(absolute)
        return out

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        soup: BeautifulSoup = decoded

        # name is the only required field; skip non-product pages outright
        name_node = soup.select_one("h1.productname")
        if name_node is None:
            return []
        name = name_node.get_text(strip=True)
        if not name:
            return []

        brand_node = soup.select_one(".manufacturers a")
        brand = brand_node.get_text(strip=True) if brand_node else None

        # price = main variant's lblPriceValue; fall back to first .productPrice on the page
        price_node = soup.select_one(
            "span[id*='ctrlProductVariantsInGrid'][id$='lblPriceValue'].productPrice"
        )
        if price_node is None:
            price_node = soup.select_one("span.productPrice")
        price = self._parse_price(price_node.get_text(" ", strip=True)) if price_node else None

        sku_node = soup.select_one("div.sku[itemprop=sku]")
        sku = self._parse_sku(sku_node.get_text(strip=True)) if sku_node else None

        return [
            Product(
                source=self.source,
                source_url=record.source_url,
                name=name,
                brand=brand,
                sku=sku,
                price=price,
                currency=self.CURRENCY,
                image_urls=self._image_urls(soup),
            )
        ]
