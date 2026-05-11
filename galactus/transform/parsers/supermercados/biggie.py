import re
import unicodedata
from decimal import Decimal
from typing import Any

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.product import Product
from sql.base import Base

_SLUG_NONALNUM = re.compile(r"[^a-z0-9]+")


class Parser(BaseParser):
    """Parses ApiSnapshots from biggie.com.py into Product entities.

    Each API page is ``{"items": [...]}``; every item with a name and code
    becomes one Product. The product page URL is rebuilt from the slugified
    name and the code (biggie's API does not return it). Prices are in
    Paraguayan guaraní. Items without a name or code are skipped
    (silver.products requires a name).
    """

    bronze_model = ApiSnapshot
    silver_model = Product

    PRODUCT_PAGE_BASE = "https://biggie.com.py/item"
    CURRENCY = "PYG"

    # lowercase, strip accents, collapse non-alphanumerics to single hyphens
    def _slugify(self, text: str) -> str:
        ascii_only = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
        return _SLUG_NONALNUM.sub("-", ascii_only.lower()).strip("-")

    # coerce a price-ish value to int; None when missing or non-numeric
    def _safe_int(self, value: Any) -> int | None:
        if not value or value in ("None", "null"):
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    # full-size image URLs for an item (type 0 = original)
    def _image_urls(self, item: dict) -> list[str]:
        return [
            im["src"] for im in (item.get("images") or []) if im.get("type") == 0 and im.get("src")
        ]

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        products: list[Base] = []
        for item in decoded.get("items", []):
            # name + code are required; the rest is best-effort
            name = (item.get("name") or "").strip()
            code = str(item.get("code") or "").strip()
            if not name or not code:
                continue

            brand = ((item.get("brand") or {}).get("name") or "").strip() or None

            # offer price wins when the item is on offer
            raw_price = item.get("price")
            if item.get("isOnOffer") and item.get("priceSaleOffer"):
                raw_price = item["priceSaleOffer"]
            price_int = self._safe_int(raw_price)
            price = Decimal(price_int) if price_int is not None else None

            products.append(
                Product(
                    source=self.source,
                    source_url=f"{self.PRODUCT_PAGE_BASE}/{self._slugify(name)}-{code}",
                    name=name,
                    brand=brand,
                    sku=code,
                    price=price,
                    currency=self.CURRENCY,
                    image_urls=self._image_urls(item),
                )
            )
        return products
