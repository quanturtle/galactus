import json

from bs4 import BeautifulSoup


def parse(html: str, url: str) -> dict | None:
    soup = BeautifulSoup(html, "lxml")

    ld = _extract_json_ld(soup)

    product_info = (
        soup.select_one("#product-info")
        or soup.select_one("[data-product-id]")
    )

    if not ld and not product_info:
        return None

    if ld:
        offer = {}
        offers = ld.get("offers", {})
        if isinstance(offers, list) and offers:
            offer = offers[0]
        elif isinstance(offers, dict):
            offer = offers

        name = str(ld.get("name", ""))
        sku = str(ld.get("sku", ""))
        price = str(offer.get("price", ""))
        description = str(ld.get("description", ""))
    else:
        name_el = soup.select_one("h2.product-title-mobile") or soup.select_one("h2")
        name = name_el.get_text(strip=True) if name_el else ""
        sku = ""
        price = product_info.get("data-product-price", "")
        description = ""

    return {
        "url": url,
        "name": name,
        "description": description,
        "price": _safe_int(price),
        "sku": sku or None,
    }


def _extract_json_ld(soup: BeautifulSoup) -> dict | None:
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            ld = json.loads(script.string)
            if ld.get("@type") == "Product":
                return ld
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue
    return None


def _safe_int(val: str | None) -> int | None:
    if not val or val in ("", "None", "null"):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None
