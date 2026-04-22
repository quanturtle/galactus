import json

from bs4 import BeautifulSoup
from galactus.parsing import build_image_urls, extract_body_images, meta

SOURCE = "cronica"


def parse(html: str, url: str) -> dict | None:
    soup = BeautifulSoup(html, "lxml")

    title = meta(soup, "og:title")
    subtitle = meta(soup, "og:description")
    published_at = meta(soup, "article:published_time")
    image_url = meta(soup, "og:image")
    author = meta(soup, "article:author")
    section = meta(soup, "article:section")

    if not author:
        author_el = soup.find(attrs={"itemprop": "author"})
        if author_el:
            name_el = author_el.find(attrs={"itemprop": "name"})
            author = name_el.get_text(strip=True) if name_el else author_el.get_text(strip=True)

    if not published_at:
        date_el = soup.find(attrs={"itemprop": "datePublished"})
        if date_el:
            published_at = date_el.get("content") or date_el.get("datetime") or date_el.get_text(strip=True)

    if not title:
        headline_el = soup.find(attrs={"itemprop": "headline"})
        if headline_el:
            title = headline_el.get_text(strip=True)

    body_el = soup.select("article p, .entry-content p, .td-post-content p")
    body = "\n\n".join(p.get_text(strip=True) for p in body_el if p.get_text(strip=True))

    body_images = extract_body_images(soup, "article, .entry-content, .td-post-content")

    if not title and not body:
        return None

    all_images = build_image_urls(image_url, body_images)
    return {
        "source": "cronica",
        "source_url": url,
        "title": title,
        "subtitle": subtitle,
        "body": body or None,
        "author": author,
        "published_at": published_at,
        "section": section,
        "image_url": image_url,
        "image_urls": json.dumps(all_images) if all_images else None,
    }
