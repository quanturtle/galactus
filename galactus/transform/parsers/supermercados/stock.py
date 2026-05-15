import re
from decimal import Decimal
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
    brand, price, and sku under stable itemprop / class hooks.

    decode() wraps the parsed soup together with the bronze source_url
    so the eight extract_* hooks need nothing else. One Product per
    bronze record, so build_item is inherited (returns [decoded]).
    """

    bronze_model = HtmlSnapshot
    silver_model = Product

    SITE_BASE = "https://www.stock.com.py"
    CURRENCY = "PYG"

    def decode(self, record: Base) -> dict:
        return {"soup": super().decode(record), "source_url": record.source_url}

    def extract_source_url(self, item: dict) -> str:
        return item["source_url"]

    # "Código de Barras:8007150902781" -> "8007150902781"
    def extract_sku(self, item: dict) -> str | None:
        node = item["soup"].select_one("div.sku[itemprop=sku]")
        if node is None:
            return None
        _, _, after = node.get_text(strip=True).partition(":")
        return after.strip() or None

    def extract_name(self, item: dict) -> str:
        node = item["soup"].select_one("h1.productname")
        return node.get_text(strip=True) if node else ""

    def extract_brand(self, item: dict) -> str | None:
        node = item["soup"].select_one(".manufacturers a")
        return node.get_text(strip=True) if node else None

    # "Gs   195.000" -> Decimal("195000"); "." is the Paraguayan thousands separator.
    # main variant's lblPriceValue; fall back to first .productPrice on the page
    def extract_price(self, item: dict) -> Decimal | None:
        soup: BeautifulSoup = item["soup"]
        node = soup.select_one(
            "span[id*='ctrlProductVariantsInGrid'][id$='lblPriceValue'].productPrice"
        )
        if node is None:
            node = soup.select_one("span.productPrice")
        if node is None:
            return None
        match = _PRICE_DIGITS.search(node.get_text(" ", strip=True))
        if not match:
            return None
        digits = match.group(0).replace(".", "")
        if not digits:
            return None
        try:
            return Decimal(digits)
        except ValueError:
            return None

    def extract_currency(self, item: dict) -> str:
        return self.CURRENCY

    def extract_unit(self, item: dict) -> str | None:
        return None

    # absolutize + dedupe image srcs from the main product slider
    def extract_image_urls(self, item: dict) -> list[str]:
        slider = item["soup"].select_one("#img-slider .ubislider-inner")
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
