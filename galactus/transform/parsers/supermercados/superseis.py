import json
from decimal import Decimal
from typing import Any

from bs4 import BeautifulSoup

from galactus.transform.base_parser import BaseParser
from galactus.transform.product_parser import ProductParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from sql.base import Base


class Parser(BaseParser, ProductParser):
    """Parses HtmlSnapshots from superseis.com.py into Product entities.

    superseis runs on a custom platform that ships a full Schema.org
    Product JSON-LD block on every product page (name, sku, brand, offers,
    image). That block is the single source of truth; OpenGraph
    ``og:image`` is kept as a fallback when JSON-LD ships no image. Pages
    without a Product JSON-LD or without a name are skipped.

    `item` here is the JSON-LD Product dict, optionally with `image`
    backfilled from og:image in build_entities.
    """

    bronze_model = HtmlSnapshot
    silver_model = Product

    DEFAULT_CURRENCY = "PYG"

    def extract_source_url(self, item: dict, record: Base) -> str:
        return record.source_url

    def extract_sku(self, item: dict, record: Base) -> str | None:
        return (item.get("sku") or "").strip() or None

    def extract_name(self, item: dict, record: Base) -> str:
        return (item.get("name") or "").strip()

    # JSON-LD `brand` is dict | str; pull the name out either way
    def extract_brand(self, item: dict, record: Base) -> str | None:
        value = item.get("brand")
        if isinstance(value, dict):
            name = (value.get("name") or "").strip()
            return name or None
        if isinstance(value, str):
            return value.strip() or None
        return None

    # JSON-LD `offers` is dict | list[dict]; first offer wins
    def _offer(self, item: dict) -> dict:
        value = item.get("offers")
        if isinstance(value, list) and value:
            first = value[0]
            return first if isinstance(first, dict) else {}
        if isinstance(value, dict):
            return value
        return {}

    # `offers.price` is a numeric string in major units (PYG has no minor unit)
    def extract_price(self, item: dict, record: Base) -> Decimal | None:
        raw = self._offer(item).get("price")
        if raw in (None, ""):
            return None
        try:
            return Decimal(str(raw))
        except (ValueError, ArithmeticError):
            return None

    def extract_currency(self, item: dict, record: Base) -> str:
        return (self._offer(item).get("priceCurrency") or self.DEFAULT_CURRENCY).strip() or self.DEFAULT_CURRENCY

    def extract_unit(self, item: dict, record: Base) -> str | None:
        return None

    # JSON-LD `image` is str | list[str]; preserve order, dedupe
    def extract_image_urls(self, item: dict, record: Base) -> list[str]:
        value = item.get("image")
        out: list[str] = []
        if isinstance(value, list):
            for entry in value:
                if isinstance(entry, str) and entry.strip() and entry not in out:
                    out.append(entry.strip())
        elif isinstance(value, str) and value.strip():
            out.append(value.strip())
        return out

    # the first <script type="application/ld+json"> describing a Product
    def _json_ld_product(self, soup: BeautifulSoup) -> dict | None:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue
            candidates: list[Any] = []
            if isinstance(data, dict):
                candidates = [data, *data.get("@graph", [])]
            elif isinstance(data, list):
                candidates = data
            for entry in candidates:
                if isinstance(entry, dict) and entry.get("@type") == "Product":
                    return entry
        return None

    # content of a <meta property=…> tag
    def _meta(self, soup: BeautifulSoup, prop: str) -> str | None:
        tag = soup.find("meta", attrs={"property": prop})
        if tag and tag.get("content"):
            return tag["content"]
        return None

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        soup: BeautifulSoup = decoded
        # JSON-LD Product is the single source of truth; skip pages without one
        product = self._json_ld_product(soup)
        if product is None:
            return []
        if not (product.get("name") or "").strip():
            return []
        # backfill image from og:image so extract_image_urls only consumes the dict
        if not product.get("image"):
            og_image = self._meta(soup, "og:image")
            if og_image:
                product = {**product, "image": og_image}
        return [self.build_entity(product, record)]
