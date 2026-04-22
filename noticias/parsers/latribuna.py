import json

from bs4 import BeautifulSoup
from galactus.parsing import build_image_urls

SOURCE = "latribuna"

BASE_URL = "https://www.latribuna.com.py"


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

    website_url = gc.get("website_url", "")
    source_url = BASE_URL + website_url if website_url else gc.get("canonical_url", "")

    hero = promo.get("url")
    all_images = build_image_urls(hero, body_images)

    return {
        "source": "latribuna",
        "source_url": source_url,
        "title": title,
        "subtitle": subheadlines.get("basic"),
        "body": "\n\n".join(body_parts) if body_parts else None,
        "author": authors[0].get("name") if authors else None,
        "published_at": gc.get("publish_date") or gc.get("display_date"),
        "section": primary_section.get("name"),
        "image_url": hero,
        "image_urls": json.dumps(all_images) if all_images else None,
        "raw_data": json.dumps(gc, ensure_ascii=False),
    }


def parse(response_text: str) -> list[dict]:
    data = json.loads(response_text)
    elements = data.get("content_elements", [])
    results = []

    for gc in elements:
        article = _parse_article(gc)
        if article:
            results.append(article)

    return results
