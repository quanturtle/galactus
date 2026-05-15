import re
import unicodedata
from decimal import Decimal
from typing import Any

from galactus.transform.base_parser import BaseParser
from galactus.transform.product_parser import ProductParser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.product import Product

_SLUG_NONALNUM = re.compile(r"[^a-z0-9]+")


class Parser(BaseParser, ProductParser):
    """Parses ApiSnapshots from biggie.com.py into Product entities.

    Each API page is ``{"items": [...]}`` — a listing payload, so
    build_item splits it into one item per entry. The product page URL
    is rebuilt from the slugified name and the code (biggie's API does
    not return it). Prices are in Paraguayan guaraní.
    """

    bronze_model = ApiSnapshot
    silver_model = Product

    PRODUCT_PAGE_BASE = "https://biggie.com.py/item"
    CURRENCY = "PYG"

    # lowercase, strip accents, collapse non-alphanumerics to single hyphens
    def extract_source_url(self, item: dict) -> str:
        name = (item.get("name") or "").strip()
        code = str(item.get("code") or "").strip()
        ascii_only = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
        slug = _SLUG_NONALNUM.sub("-", ascii_only.lower()).strip("-")
        return f"{self.PRODUCT_PAGE_BASE}/{slug}-{code}"

    def extract_sku(self, item: dict) -> str | None:
        return str(item.get("code") or "").strip() or None

    def extract_name(self, item: dict) -> str:
        return (item.get("name") or "").strip()

    def extract_brand(self, item: dict) -> str | None:
        return ((item.get("brand") or {}).get("name") or "").strip() or None

    # offer price wins when the item is on offer; coerce price-ish value to int
    def extract_price(self, item: dict) -> Decimal | None:
        raw = item.get("price")
        if item.get("isOnOffer") and item.get("priceSaleOffer"):
            raw = item["priceSaleOffer"]
        if not raw or raw in ("None", "null"):
            return None
        try:
            return Decimal(int(float(raw)))
        except (ValueError, TypeError):
            return None

    def extract_currency(self, item: dict) -> str:
        return self.CURRENCY

    def extract_unit(self, item: dict) -> str | None:
        return None

    # full-size image URLs for an item (type 0 = original)
    def extract_image_urls(self, item: dict) -> list[str]:
        return [
            im["src"] for im in (item.get("images") or []) if im.get("type") == 0 and im.get("src")
        ]

    def build_item(self, decoded: Any) -> list[dict]:
        return [entry for entry in decoded.get("items", []) if isinstance(entry, dict)]
