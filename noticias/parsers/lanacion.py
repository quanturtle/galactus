import json

from bs4 import BeautifulSoup
from galactus.parsing import build_image_urls

SOURCE = "lanacion"

BASE_URL = "https://www.lanacion.com.py"


def _parse_article(gc: dict) -> dict | None:
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

    canonical = gc.get("canonical_url", "")
    source_url = BASE_URL + canonical if canonical.startswith("/") else canonical

    hero = promo.get("url")
    all_images = build_image_urls(hero, body_images)

    return {
        "source": "lanacion",
        "source_url": source_url,
        "title": title,
        "subtitle": subheadlines.get("basic"),
        "body": body,
        "author": authors[0].get("name") if authors else None,
        "published_at": gc.get("publish_date") or gc.get("display_date"),
        "section": primary_section.get("name"),
        "image_url": hero,
        "image_urls": json.dumps(all_images) if all_images else None,
    }


def parse(response_text: str) -> list[dict]:
    data = json.loads(response_text)
    results = []

    for gc in data.get("content_elements", []):
        article = _parse_article(gc)
        if article:
            results.append(article)

    return results
