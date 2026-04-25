import json

from galactus.parsing import safe_int, slugify

SOURCE = "biggie"

_PRODUCT_PAGE_BASE = "https://biggie.com.py/item"


def _build_url(name: str, code: str) -> str:
    return f"{_PRODUCT_PAGE_BASE}/{slugify(name)}-{code}"


def _full_size_image_urls(item: dict) -> list[str]:
    return [
        im["src"]
        for im in (item.get("images") or [])
        if im.get("type") == 0 and im.get("src")
    ]


def transform(response_text: str) -> list[dict]:
    """Parse a Biggie API response page into silver-ready product dicts."""
    data = json.loads(response_text)
    results = []

    for item in data.get("items", []):
        name = item.get("name", "").strip()
        code = str(item.get("code", "")).strip()
        if not name or not code:
            continue

        brand = (item.get("brand") or {}).get("name", "").strip()
        family = item.get("family") or {}
        family_name = family.get("name", "").strip()
        classification = (family.get("classification") or {}).get("name", "").strip()

        parts = [p for p in [brand, classification, family_name] if p]
        description = " > ".join(parts) if parts else None

        price = item.get("price")
        if item.get("isOnOffer") and item.get("priceSaleOffer"):
            price = item["priceSaleOffer"]

        results.append({
            "url": _build_url(name, code),
            "name": name,
            "description": description,
            "price": safe_int(price),
            "sku": code,
            "images": _full_size_image_urls(item),
        })

    return results
