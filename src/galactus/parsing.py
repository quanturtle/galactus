"""Reusable helpers for parsing scraped HTML and API responses."""

import json
import re

from bs4 import BeautifulSoup

IMAGE_EXCLUDE = {"logo", "icon", "avatar", "emoji", "gravatar", "sprite", "pixel", "tracking", "badge"}


def meta(soup: BeautifulSoup, prop: str) -> str | None:
    """Extract content from a <meta> tag by property or name."""
    tag = soup.find("meta", attrs={"property": prop}) or soup.find("meta", attrs={"name": prop})
    return tag["content"] if tag and tag.get("content") else None


def extract_json_ld(soup: BeautifulSoup) -> dict | None:
    """Extract the first NewsArticle/Article JSON-LD block, including @graph."""
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


def extract_body_images(soup: BeautifulSoup, selectors: str) -> list[str]:
    """Extract image URLs from a container matching *selectors*."""
    images: list[str] = []
    container = soup.select_one(selectors)
    if not container:
        return images
    for img in container.select("img[src]"):
        src = img.get("src", "")
        if not src.startswith("http"):
            continue
        if any(kw in src.lower() for kw in IMAGE_EXCLUDE):
            continue
        if src not in images:
            images.append(src)
    return images


def build_image_urls(hero: str | None, body_images: list[str]) -> list[str] | None:
    """Merge hero image with body images, preserving order and deduplicating."""
    all_imgs: list[str] = []
    if hero:
        all_imgs.append(hero)
    for img in body_images:
        if img not in all_imgs:
            all_imgs.append(img)
    return all_imgs if all_imgs else None


def safe_int(val: str | None) -> int | None:
    """Convert a value to int, returning None for missing/invalid values."""
    if not val or val in ("", "None", "null"):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def extract_var_data(html: str) -> dict | None:
    """Extract key-value pairs from an inline ``var data = {…}`` script block."""
    match = re.search(r"var\s+data\s*=\s*\{(.+?)\}", html, re.DOTALL)
    if not match:
        return None
    pairs = re.findall(r"(\w+)\s*:\s*'([^']*)'", match.group(1))
    return dict(pairs) if pairs else None
