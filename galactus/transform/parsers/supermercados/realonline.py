from decimal import Decimal
from typing import Any

from galactus.transform.base_parser import BaseParser
from galactus.transform.product_parser import ProductParser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.product import Product


class Parser(BaseParser, ProductParser):
    """Parses ApiSnapshots from the realonline Instaleap API into Product entities.

    Each ApiSnapshot is one getProductsByCategory page — a listing payload, so
    build_item splits category.products into one item per product. Prices are
    integer guaraní. The product page URL is rebuilt from the API slug, which
    already ends with the product id and maps 1:1 to the site's /p/ path.
    """

    bronze_model = ApiSnapshot
    silver_model = Product

    PRODUCT_PAGE_BASE = "https://www.realonline.com.py/p"
    CURRENCY = "PYG"

    def extract_source_url(self, item: dict) -> str:
        return f"{self.PRODUCT_PAGE_BASE}/{(item.get('slug') or '').strip()}"

    def extract_sku(self, item: dict) -> str | None:
        return (item.get("sku") or "").strip() or None

    def extract_name(self, item: dict) -> str:
        return (item.get("name") or "").strip()

    def extract_brand(self, item: dict) -> str | None:
        return (item.get("brand") or "").strip() or None

    # promotionPricePerSubUnit is the per-unit promo price (same basis as price); it wins when set
    def extract_price(self, item: dict) -> Decimal | None:
        promotion_price = item.get("promotionPricePerSubUnit")
        raw = promotion_price if promotion_price is not None else item.get("price")
        if raw is None:
            return None
        try:
            return Decimal(int(raw))
        except (ValueError, TypeError):
            return None

    def extract_currency(self, item: dict) -> str:
        return self.CURRENCY

    # the API "unit" field is store-entered free text (EANs, stock counts leak in) — parse the name instead
    def extract_unit(self, item: dict) -> str | None:
        return self.parse_unit_from_name(self.extract_name(item))

    def extract_image_urls(self, item: dict) -> list[str]:
        return [url for url in (item.get("photosUrl") or []) if url]

    def build_item(self, decoded: Any) -> list[dict]:
        result = (decoded.get("data") or {}).get("getProductsByCategory") or {}
        category = result.get("category") or {}
        return [product for product in (category.get("products") or []) if isinstance(product, dict)]
