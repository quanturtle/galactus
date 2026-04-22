import json

from the_scraper.parsing import safe_int

SOURCE = "biggie"


def parse(response_text: str) -> list[dict]:
    """Parse a Biggie API response page into silver-ready product dicts."""
    data = json.loads(response_text)
    results = []

    for item in data.get("items", []):
        name = item.get("name", "").strip()
        if not name:
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
            "url": f"api://biggie/{item['id']}",
            "name": name,
            "description": description,
            "price": safe_int(price),
            "sku": str(item.get("code", "")) or None,
        })

    return results
