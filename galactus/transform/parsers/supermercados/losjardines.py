import re
from decimal import Decimal

from galactus.transform.base_parser import BaseParser
from galactus.transform.product_parser import ProductParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from sql.base import Base

CDN_IMAGE_TEMPLATE = "https://losjardines.cdn1.dattamax.com/userfiles/images/productos/600/{sku}.jpg"


class Parser(BaseParser, ProductParser):
    """Parses HtmlSnapshots from losjardinesonline.com.py into Product entities.

    losjardines runs on the Dattamax SaaS platform (same backend as casarica
    and arete), but each product page exposes name, EAN, and price as
    data-attributes on ``<button id="lnk-add-to-cart">``; we read those
    directly. The h1 fallback for name covers pages where the cart button
    is absent (out-of-stock variants). Image URLs are reconstructed from
    the EAN on the Dattamax CDN.

    One Product per bronze record, so build_item is inherited.
    """

    bronze_model = HtmlSnapshot
    silver_model = Product

    CURRENCY = "PYG"

    def decode(self, record: Base) -> dict:
        soup = super().decode(record)
        cart = soup.select_one("button#lnk-add-to-cart[data-product_id]")
        attrs = dict(cart.attrs) if cart else {}
        return {"soup": soup, "attrs": attrs, "source_url": record.request_url}

    def extract_source_url(self, item: dict) -> str:
        return item["source_url"]

    def extract_sku(self, item: dict) -> str | None:
        ean = (item["attrs"].get("data-product_ean") or "").strip()
        return ean or None

    def extract_name(self, item: dict) -> str:
        name = (item["attrs"].get("data-product_name") or "").strip()
        if name:
            return name
        h1 = item["soup"].select_one("h1.product_title")
        return h1.get_text(" ", strip=True) if h1 else ""

    # brand is the breadcrumb "marca" crumb — its href ends in -m<id>
    # (category crumbs end in -c<id>, the product crumb in -p<id>)
    def extract_brand(self, item: dict) -> str | None:
        for crumb in item["soup"].select("nav.ecommercepro-breadcrumb a[href]"):
            if re.search(r"-m\d+/?$", crumb["href"]):
                return crumb.get_text(" ", strip=True) or None
        return None

    # data-product_price is an integer string in PYG (e.g. "15900") — no separators to strip
    def extract_price(self, item: dict) -> Decimal | None:
        raw = (item["attrs"].get("data-product_price") or "").strip()
        if not raw:
            return None
        try:
            return Decimal(raw)
        except (ValueError, ArithmeticError):
            return None

    def extract_currency(self, item: dict) -> str:
        return self.CURRENCY

    def extract_unit(self, item: dict) -> str | None:
        return self.parse_unit_from_name(self.extract_name(item))

    # image is reconstructed from the EAN on Dattamax's CDN; absent without one
    def extract_image_urls(self, item: dict) -> list[str]:
        sku = self.extract_sku(item)
        if not sku:
            return []
        return [CDN_IMAGE_TEMPLATE.format(sku=sku)]
