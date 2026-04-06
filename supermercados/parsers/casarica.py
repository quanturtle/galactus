import re


def parse(html: str, url: str) -> dict | None:
    data = _extract_var_data(html)
    if not data:
        return None

    return {
        "url": url,
        "name": data.get("name", ""),
        "description": data.get("category", ""),
        "price": _safe_int(data.get("price", "")),
        "sku": data.get("ean") or None,
    }


def _extract_var_data(html: str) -> dict | None:
    match = re.search(r"var\s+data\s*=\s*\{(.+?)\}", html, re.DOTALL)
    if not match:
        return None
    pairs = re.findall(r"(\w+)\s*:\s*'([^']*)'", match.group(1))
    return dict(pairs) if pairs else None


def _safe_int(val: str | None) -> int | None:
    if not val or val in ("", "None", "null"):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None
