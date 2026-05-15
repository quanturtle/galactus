import re
from decimal import Decimal
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from galactus.transform.base_parser import BaseParser
from galactus.transform.product_parser import ProductParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from sql.base import Base

_PRICE_DIGITS = re.compile(r"[\d.]+")


class Parser(BaseParser, ProductParser):
    """Parses HtmlSnapshots from stock.com.py into Product entities.

    Stock is a classic ASP.NET shop; each /products/ page exposes name,
    brand, price, and sku under stable itemprop / class hooks. Pages
    missing the name are skipped (silver.products requires it).
    """

    bronze_model = HtmlSnapshot
    silver_model = Product

    SITE_BASE = "https://www.stock.com.py"
    CURRENCY = "PYG"

    def extract_source_url(self, item: BeautifulSoup, record: Base) -> str:
        return record.source_url

    # "Código de Barras:8007150902781" -> "8007150902781"
    def _parse_sku(self, text: str) -> str | None:
        _, _, after = text.partition(":")
        sku = after.strip()
        return sku or None

    def extract_sku(self, item: BeautifulSoup, record: Base) -> str | None:
        node = item.select_one("div.sku[itemprop=sku]")
        return self._parse_sku(node.get_text(strip=True)) if node else None

    # presence guaranteed by build_entities; re-selects to stay self-contained
    def extract_name(self, item: BeautifulSoup, record: Base) -> str:
        node = item.select_one("h1.productname")
        return node.get_text(strip=True) if node else ""

    def extract_brand(self, item: BeautifulSoup, record: Base) -> str | None:
        node = item.select_one(".manufacturers a")
        return node.get_text(strip=True) if node else None

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

    def extract_price(self, item: BeautifulSoup, record: Base) -> Decimal | None:
        # main variant's lblPriceValue; fall back to first .productPrice on the page
        node = item.select_one(
            "span[id*='ctrlProductVariantsInGrid'][id$='lblPriceValue'].productPrice"
        )
        if node is None:
            node = item.select_one("span.productPrice")
        return self._parse_price(node.get_text(" ", strip=True)) if node else None

    def extract_currency(self, item: BeautifulSoup, record: Base) -> str:
        return self.CURRENCY

    def extract_unit(self, item: BeautifulSoup, record: Base) -> str | None:
        return None

    # absolutize + dedupe image srcs from the main product slider
    def extract_image_urls(self, item: BeautifulSoup, record: Base) -> list[str]:
        slider = item.select_one("#img-slider .ubislider-inner")
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
        if name_node is None or not name_node.get_text(strip=True):
            return []
        return [self.build_entity(soup, record)]
