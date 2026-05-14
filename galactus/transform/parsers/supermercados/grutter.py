from decimal import Decimal
from typing import Any

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.product import Product
from sql.base import Base


class Parser(BaseParser):
    """Parses ApiSnapshots from grutter (WooCommerce Store API) into Product entities.

    The bronze body is a ``/wp-json/wc/store/v1/products`` page — a JSON
    array of WC product objects. Each becomes one Product. Prices are
    strings in minor units, scaled by ``prices.currency_minor_unit``;
    PYG runs at 0 minor units so the value passes through as an integer.
    Items without a name are skipped (silver.products requires it).
    """

    bronze_model = ApiSnapshot
    silver_model = Product

    # `prices.price` (string, minor units) / 10**currency_minor_unit; None on missing/bad
    def _price(self, prices: dict) -> Decimal | None:
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

    # `images[].src` URLs in feed order, deduped
    def _image_urls(self, item: dict) -> list[str]:
        out: list[str] = []
        for img in item.get("images") or []:
            if not isinstance(img, dict):
                continue
            src = (img.get("src") or "").strip()
            if src and src not in out:
                out.append(src)
        return out

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        # WC Store /products returns a JSON array; tolerate empty/odd pages
        if not isinstance(decoded, list):
            return []

        products: list[Base] = []
        for item in decoded:
            if not isinstance(item, dict):
                continue

            # name is required; skip items without one
            name = (item.get("name") or "").strip()
            if not name:
                continue

            sku = (item.get("sku") or "").strip() or None
            permalink = (item.get("permalink") or "").strip()

            prices = item.get("prices") or {}
            price = self._price(prices)
            currency = (prices.get("currency_code") or "PYG").strip() or "PYG"

            products.append(
                Product(
                    source=self.source,
                    source_url=permalink,
                    name=name,
                    sku=sku,
                    price=price,
                    currency=currency,
                    image_urls=self._image_urls(item),
                )
            )
        return products
