import html
import json

from galactus.parsing import safe_int

SOURCE = "grutter"


def _image_urls(item: dict) -> list[str]:
    return [im["src"] for im in (item.get("images") or []) if im.get("src")]


def transform(response_text: str) -> list[dict]:
    """Parse a Grutter API response page into silver-ready product dicts."""
    items = json.loads(response_text)
    results = []

    for item in items:
        name = html.unescape(item.get("name", "")).strip()
        permalink = (item.get("permalink") or "").strip()
        if not name or not permalink:
            continue

        prices = item.get("prices") or {}
        categories = item.get("categories") or []
        category = categories[0]["name"] if categories else None

        results.append({
            "url": permalink,
            "name": name,
            "description": category,
            "price": safe_int(prices.get("price")),
            "sku": str(item.get("sku", "")) or None,
            "images": _image_urls(item),
        })

    return results
