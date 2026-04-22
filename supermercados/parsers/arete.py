from the_scraper.parsing import extract_var_data, safe_int

SOURCE = "arete"


def parse(html: str, url: str) -> dict | None:
    data = extract_var_data(html)
    if not data:
        return None

    return {
        "url": url,
        "name": data.get("name", ""),
        "description": data.get("category", ""),
        "price": safe_int(data.get("price", "")),
        "sku": data.get("ean") or None,
    }