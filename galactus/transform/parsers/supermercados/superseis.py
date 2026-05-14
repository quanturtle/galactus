import json
from decimal import Decimal
from typing import Any

from bs4 import BeautifulSoup

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from sql.base import Base


class Parser(BaseParser):
    """Parses HtmlSnapshots from superseis.com.py into Product entities.

    superseis runs on a custom platform that ships a full Schema.org
    Product JSON-LD block on every product page (name, sku, brand, offers,
    image). That block is the single source of truth; OpenGraph
    ``og:image`` is kept as a fallback when JSON-LD ships no image. Pages
    without a Product JSON-LD or without a name are skipped.
    """

    bronze_model = HtmlSnapshot
    silver_model = Product

    DEFAULT_CURRENCY = "PYG"

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
            for item in candidates:
                if isinstance(item, dict) and item.get("@type") == "Product":
                    return item
        return None

    # content of a <meta property=…> tag
    def _meta(self, soup: BeautifulSoup, prop: str) -> str | None:
        tag = soup.find("meta", attrs={"property": prop})
        if tag and tag.get("content"):
            return tag["content"]
        return None

    # JSON-LD `brand` is dict | str; pull the name out either way
    def _brand(self, value: Any) -> str | None:
        if isinstance(value, dict):
            name = (value.get("name") or "").strip()
            return name or None
        if isinstance(value, str):
            return value.strip() or None
        return None

    # JSON-LD `offers` is dict | list[dict]; first offer wins
    def _offer(self, value: Any) -> dict:
        if isinstance(value, list) and value:
            first = value[0]
            return first if isinstance(first, dict) else {}
        if isinstance(value, dict):
            return value
        return {}

    # `offers.price` is a numeric string in major units (PYG has no minor unit)
    def _price(self, offer: dict) -> Decimal | None:
        raw = offer.get("price")
        if raw in (None, ""):
            return None
        try:
            return Decimal(str(raw))
        except (ValueError, ArithmeticError):
            return None

    # JSON-LD `image` is str | list[str]; preserve order, dedupe
    def _image_urls(self, value: Any) -> list[str]:
        out: list[str] = []
        if isinstance(value, list):
            for entry in value:
                if isinstance(entry, str) and entry.strip() and entry not in out:
                    out.append(entry.strip())
        elif isinstance(value, str) and value.strip():
            out.append(value.strip())
        return out

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        soup: BeautifulSoup = decoded

        # JSON-LD Product is the single source of truth; skip pages without one
        product = self._json_ld_product(soup)
        if product is None:
            return []
        name = (product.get("name") or "").strip()
        if not name:
            return []

        sku = (product.get("sku") or "").strip() or None
        brand = self._brand(product.get("brand"))

        offer = self._offer(product.get("offers"))
        price = self._price(offer)
        currency = (offer.get("priceCurrency") or self.DEFAULT_CURRENCY).strip() or self.DEFAULT_CURRENCY

        # image_urls: JSON-LD field; fall back to og:image when JSON-LD ships none
        image_urls = self._image_urls(product.get("image"))
        if not image_urls:
            og_image = self._meta(soup, "og:image")
            if og_image:
                image_urls.append(og_image)

        return [
            Product(
                source=self.source,
                source_url=record.source_url,
                name=name,
                sku=sku,
                brand=brand,
                price=price,
                currency=currency,
                image_urls=image_urls,
            )
        ]
