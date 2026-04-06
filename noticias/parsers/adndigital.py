import json

from bs4 import BeautifulSoup


def parse(html: str, url: str) -> dict | None:
    soup = BeautifulSoup(html, "lxml")

    json_ld = _extract_json_ld(soup)
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
        title = _meta(soup, "og:title")
    if not published_at:
        published_at = _meta(soup, "article:published_time")
    if not image_url:
        image_url = _meta(soup, "og:image")

    section = _meta(soup, "article:section")
    subtitle = _meta(soup, "og:description")

    body_el = soup.select("article p, .entry-content p, .post-content p")
    body = "\n\n".join(p.get_text(strip=True) for p in body_el if p.get_text(strip=True))

    body_images = _extract_body_images(soup, "article, .entry-content, .post-content")

    if not title and not body:
        return None

    all_images = _build_image_urls(image_url, body_images)
    return {
        "source": "adndigital",
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


def _extract_json_ld(soup: BeautifulSoup) -> dict | None:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict):
                if data.get("@type") in ("NewsArticle", "Article"):
                    return data
                for item in data.get("@graph", []):
                    if isinstance(item, dict) and item.get("@type") in ("NewsArticle", "Article"):
                        return item
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") in ("NewsArticle", "Article"):
                        return item
        except json.JSONDecodeError:
            continue
    return None


def _meta(soup: BeautifulSoup, prop: str) -> str | None:
    tag = soup.find("meta", attrs={"property": prop}) or soup.find("meta", attrs={"name": prop})
    return tag["content"] if tag and tag.get("content") else None


_IMAGE_EXCLUDE = {"logo", "icon", "avatar", "emoji", "gravatar", "sprite", "pixel", "tracking", "badge"}


def _extract_body_images(soup: BeautifulSoup, selectors: str) -> list[str]:
    images: list[str] = []
    container = soup.select_one(selectors)
    if not container:
        return images
    for img in container.select("img[src]"):
        src = img.get("src", "")
        if not src.startswith("http"):
            continue
        if any(kw in src.lower() for kw in _IMAGE_EXCLUDE):
            continue
        if src not in images:
            images.append(src)
    return images


def _build_image_urls(hero: str | None, body_images: list[str]) -> list[str] | None:
    all_imgs: list[str] = []
    if hero:
        all_imgs.append(hero)
    for img in body_images:
        if img not in all_imgs:
            all_imgs.append(img)
    return all_imgs if all_imgs else None
