import json

from bs4 import BeautifulSoup


def parse(html: str, url: str) -> dict | None:
    soup = BeautifulSoup(html, "lxml")

    title = _meta(soup, "og:title")
    subtitle = _meta(soup, "og:description")
    published_at = _meta(soup, "article:published_time")
    image_url = _meta(soup, "og:image")
    author = _meta(soup, "article:author")
    section = _meta(soup, "article:section")

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

    body_images = _extract_body_images(soup, "article, .entry-content, .td-post-content")

    if not title and not body:
        return None

    all_images = _build_image_urls(image_url, body_images)
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
