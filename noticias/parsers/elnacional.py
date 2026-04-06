import json

from bs4 import BeautifulSoup
from the_scraper.parsing import build_image_urls, extract_body_images, extract_json_ld, meta


def parse(html: str, url: str) -> dict | None:
    soup = BeautifulSoup(html, "lxml")

    json_ld = extract_json_ld(soup)
    title = None
    author = None
    published_at = None
    image_url = None
    raw_data = None

    if json_ld:
        title = json_ld.get("headline") or json_ld.get("name")
        published_at = json_ld.get("datePublished")
        authors = json_ld.get("author")
        if isinstance(authors, list) and authors:
            author = authors[0].get("name") if isinstance(authors[0], dict) else str(authors[0])
        elif isinstance(authors, dict):
            author = authors.get("name")
        images = json_ld.get("image")
        if isinstance(images, list) and images:
            img = images[0]
            image_url = img.get("url") if isinstance(img, dict) else img
        elif isinstance(images, dict):
            image_url = images.get("url")
        elif isinstance(images, str):
            image_url = images
        raw_data = json.dumps(json_ld, ensure_ascii=False)

    if not title:
        title = meta(soup, "og:title")
    if not published_at:
        published_at = meta(soup, "article:published_time")
    if not image_url:
        image_url = meta(soup, "og:image")

    section = meta(soup, "article:section")
    subtitle = meta(soup, "og:description")

    body_el = soup.select("article p, .entry-content p, .article-content p, .post-content p")
    body = "\n\n".join(p.get_text(strip=True) for p in body_el if p.get_text(strip=True))

    body_images = extract_body_images(soup, "article, .entry-content, .article-content, .post-content")

    if not title and not body:
        return None

    all_images = build_image_urls(image_url, body_images)
    return {
        "source": "elnacional",
        "source_url": url,
        "title": title,
        "subtitle": subtitle,
        "body": body or None,
        "author": author,
        "published_at": published_at,
        "section": section,
        "image_url": image_url,
        "image_urls": json.dumps(all_images) if all_images else None,
        "raw_data": raw_data,
    }
