from decimal import Decimal
from typing import Any

from galactus.transform.base_parser import BaseParser
from galactus.transform.product_parser import ProductParser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.product import Product


class Parser(BaseParser, ProductParser):
    """Parses ApiSnapshots from grutter (WooCommerce Store API) into Product entities.

    The bronze body is a ``/wp-json/wc/store/v1/products`` page — a JSON
    array of WC product objects. build_item splits the array into one
    item per entry. Prices are strings in minor units, scaled by
    ``prices.currency_minor_unit``; PYG runs at 0 minor units so the
    value passes through as an integer.
    """

    bronze_model = ApiSnapshot
    silver_model = Product

    DEFAULT_CURRENCY = "PYG"

    def extract_source_url(self, item: dict) -> str:
        return (item.get("permalink") or "").strip()

    def extract_sku(self, item: dict) -> str | None:
        return (item.get("sku") or "").strip() or None

    def extract_name(self, item: dict) -> str:
        return (item.get("name") or "").strip()

    def extract_brand(self, item: dict) -> str | None:
        return None

    # `prices.price` (string, minor units) / 10**currency_minor_unit; None on missing/bad
    def extract_price(self, item: dict) -> Decimal | None:
        prices = item.get("prices") or {}
        raw = prices.get("price")
        if raw in (None, ""):
            return None
        try:
            value = Decimal(str(raw))
        except (ValueError, ArithmeticError):
            return None
        minor = prices.get("currency_minor_unit") or 0
        if minor:
            value = value / (Decimal(10) ** minor)
        return value

    def extract_currency(self, item: dict) -> str:
        prices = item.get("prices") or {}
        return (
            prices.get("currency_code") or self.DEFAULT_CURRENCY
        ).strip() or self.DEFAULT_CURRENCY

    def extract_unit(self, item: dict) -> str | None:
        return self.parse_unit_from_name(self.extract_name(item))

    # `images[].src` URLs in feed order, deduped
    def extract_image_urls(self, item: dict) -> list[str]:
        out: list[str] = []
        for img in item.get("images") or []:
            if not isinstance(img, dict):
                continue
            src = (img.get("src") or "").strip()
            if src and src not in out:
                out.append(src)
        return out

    def build_item(self, decoded: Any) -> list[dict]:
        # WC Store /products returns a JSON array; tolerate empty/odd pages
        if not isinstance(decoded, list):
            return []
        return [entry for entry in decoded if isinstance(entry, dict)]
