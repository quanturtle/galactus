import json
import re

from bs4 import BeautifulSoup
from the_scraper.parsing import build_image_urls

SOURCE = "lanacion"

_FUSION_RE = re.compile(r"Fusion\.globalContent\s*=\s*(\{.*?\});", re.DOTALL)


def parse(html: str, url: str) -> dict | None:
    m = _FUSION_RE.search(html)
    if not m:
        return None
    try:
        gc = json.loads(m.group(1))
    except json.JSONDecodeError:
        return None

    headlines = gc.get("headlines", {})
    title = headlines.get("basic")
    if not title:
        return None

    subheadlines = gc.get("subheadlines", {})
    credits = gc.get("credits", {})
    authors = credits.get("by", [])
    taxonomy = gc.get("taxonomy", {})
    primary_section = taxonomy.get("primary_section", {})
    promo = gc.get("promo_items", {}).get("basic", {})

    body_parts = []
    body_images = []
    for el in gc.get("content_elements", []):
        if el.get("type") == "text" and el.get("content"):
            soup = BeautifulSoup(el["content"], "lxml")
            body_parts.append(soup.get_text())
        elif el.get("type") == "image" and el.get("url"):
            body_images.append(el["url"])

    body = "\n\n".join(body_parts) if body_parts else gc.get("description", {}).get("basic")

    hero = promo.get("url")
    all_images = build_image_urls(hero, body_images)

    return {
        "source": "lanacion",
        "source_url": url,
        "title": title,
        "subtitle": subheadlines.get("basic"),
        "body": body,
        "author": authors[0].get("name") if authors else None,
        "published_at": gc.get("publish_date") or gc.get("display_date"),
        "section": primary_section.get("name"),
        "image_url": hero,
        "image_urls": json.dumps(all_images) if all_images else None,
    }
