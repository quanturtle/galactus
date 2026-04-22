import json

from bs4 import BeautifulSoup
from the_scraper.parsing import IMAGE_EXCLUDE, build_image_urls

SOURCE = "hoy"


def _strip_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    return soup.get_text(strip=True)


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    paragraphs = soup.find_all("p")
    if paragraphs:
        return "\n\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
    return soup.get_text(strip=True)


def _parse_post(post: dict) -> dict | None:
    title = _strip_html(post.get("title", {}).get("rendered", ""))
    if not title:
        return None

    content_html = post.get("content", {}).get("rendered", "")
    body = _html_to_text(content_html) if content_html else None

    body_images = []
    if content_html:
        body_soup = BeautifulSoup(content_html, "lxml")
        for img in body_soup.select("img[src]"):
            src = img.get("src", "")
            if src.startswith("http") and not any(kw in src.lower() for kw in IMAGE_EXCLUDE):
                if src not in body_images:
                    body_images.append(src)

    subtitle = _strip_html(post.get("excerpt", {}).get("rendered", ""))

    author = None
    image_url = None
    section = None

    embedded = post.get("_embedded", {})

    authors = embedded.get("author", [])
    if authors and isinstance(authors[0], dict):
        author = authors[0].get("name")

    media = embedded.get("wp:featuredmedia", [])
    if media and isinstance(media[0], dict):
        image_url = media[0].get("source_url")

    terms = embedded.get("wp:term", [])
    if terms and isinstance(terms[0], list) and terms[0]:
        section = terms[0][0].get("name")

    all_images = build_image_urls(image_url, body_images)
    return {
        "source": "hoy",
        "source_url": post.get("link", ""),
        "title": title,
        "subtitle": subtitle or None,
        "body": body or None,
        "author": author,
        "published_at": post.get("date"),
        "section": section,
        "image_url": image_url,
        "image_urls": json.dumps(all_images) if all_images else None,
        "raw_data": json.dumps(post, ensure_ascii=False, default=str),
    }


def parse(response_text: str) -> list[dict]:
    posts = json.loads(response_text)
    results = []

    for post in posts:
        article = _parse_post(post)
        if article:
            results.append(article)

    return results
