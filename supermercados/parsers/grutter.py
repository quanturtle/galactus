import html
import json

from galactus.parsing import safe_int

SOURCE = "grutter"


def parse(response_text: str) -> list[dict]:
    """Parse a Grutter API response page into silver-ready product dicts."""
    items = json.loads(response_text)
    results = []

    for item in items:
        name = html.unescape(item.get("name", "")).strip()
        if not name:
            continue

        prices = item.get("prices") or {}
        categories = item.get("categories") or []
        category = categories[0]["name"] if categories else None

        results.append({
            "url": f"api://grutter/{item['id']}",
            "name": name,
            "description": category,
            "price": safe_int(prices.get("price")),
            "sku": str(item.get("sku", "")) or None,
        })

    return results
