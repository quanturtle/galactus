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
    ``og:image`` is kept as a fallback when JSON-LD ships no image.

    decode() extracts the JSON-LD Product dict (or an empty dict when no
    Product is present), backfills ``image`` from og:image when missing,
    and bundles it with the bronze source_url so the eight extract_*
    hooks need nothing else. One Product per bronze record, so
    build_item is inherited (returns [decoded]).
    """

    bronze_model = HtmlSnapshot
    silver_model = Product

    DEFAULT_CURRENCY = "PYG"

    def decode(self, record: Base) -> dict:
        soup: BeautifulSoup = super().decode(record)

        # find the first <script type="application/ld+json"> describing a Product
        product: dict = {}
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
            found = next(
                (e for e in candidates if isinstance(e, dict) and e.get("@type") == "Product"),
                None,
            )
            if found is not None:
                product = found
                break

        # backfill image from og:image so extract_image_urls only consumes the dict
        if product and not product.get("image"):
            og_tag = soup.find("meta", attrs={"property": "og:image"})
            og_image = og_tag.get("content") if og_tag else None
            if og_image:
                product = {**product, "image": og_image}

        return {"product": product, "source_url": record.source_url}

    def extract_source_url(self, item: dict) -> str:
        return item["source_url"]

    def extract_sku(self, item: dict) -> str | None:
        return (item["product"].get("sku") or "").strip() or None

    def extract_name(self, item: dict) -> str:
        return (item["product"].get("name") or "").strip()

    # JSON-LD `brand` is dict | str; pull the name out either way
    def extract_brand(self, item: dict) -> str | None:
        value = item["product"].get("brand")
        if isinstance(value, dict):
            name = (value.get("name") or "").strip()
            return name or None
        if isinstance(value, str):
            return value.strip() or None
        return None

    # JSON-LD `offers` is dict | list[dict]; first offer wins.
    # `offers.price` is a numeric string in major units (PYG has no minor unit)
    def extract_price(self, item: dict) -> Decimal | None:
        offers = item["product"].get("offers")
        offer = offers[0] if isinstance(offers, list) and offers else offers
        if not isinstance(offer, dict):
            return None
        raw = offer.get("price")
        if raw in (None, ""):
            return None
        try:
            return Decimal(str(raw))
        except (ValueError, ArithmeticError):
            return None

    def extract_currency(self, item: dict) -> str:
        offers = item["product"].get("offers")
        offer = offers[0] if isinstance(offers, list) and offers else offers
        if not isinstance(offer, dict):
            return self.DEFAULT_CURRENCY
        return (offer.get("priceCurrency") or self.DEFAULT_CURRENCY).strip() or self.DEFAULT_CURRENCY

    def extract_unit(self, item: dict) -> str | None:
        return None

    # JSON-LD `image` is str | list[str]; preserve order, dedupe
    def extract_image_urls(self, item: dict) -> list[str]:
        value = item["product"].get("image")
        out: list[str] = []
        if isinstance(value, list):
            for entry in value:
                if isinstance(entry, str) and entry.strip() and entry not in out:
                    out.append(entry.strip())
        elif isinstance(value, str) and value.strip():
            out.append(value.strip())
        return out
