import json
from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from galactus.transform.article_parser import ArticleParser
from galactus.transform.base_parser import BaseParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.article import Article
from sql.base import Base

# substrings that mark a non-editorial image (logos, icons, tracking pixels…)
IMAGE_EXCLUDE = frozenset(
    {"logo", "icon", "avatar", "emoji", "gravatar", "sprite", "pixel", "tracking", "badge"}
)


# the first <script type="application/ld+json"> describing a NewsArticle/Article,
# or an empty dict when the page ships no such block
def _find_json_ld(soup: BeautifulSoup) -> dict:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        candidates: list[Any] = []
        if isinstance(data, dict):
            candidates = [data, *data.get("@graph", [])]
        elif isinstance(data, list):
            candidates = data
        for entry in candidates:
            if isinstance(entry, dict) and entry.get("@type") in ("NewsArticle", "Article"):
                return entry
    return {}


# content of a <meta property=…> / <meta name=…> tag
def _meta_content(soup: BeautifulSoup, prop: str) -> str | None:
    tag = soup.find("meta", attrs={"property": prop}) or soup.find("meta", attrs={"name": prop})
    if tag and tag.get("content"):
        return tag["content"]
    return None


# parse a published-at string; None if missing or unparseable
def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return dateparser.parse(value)
    except (ValueError, OverflowError, TypeError):
        return None


# JSON-LD `image` is dict | list | str; pick the first url
def _ld_image_url(value: Any) -> str | None:
    if isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, dict):
            return first.get("url")
        if isinstance(first, str):
            return first
    if isinstance(value, dict):
        return value.get("url")
    if isinstance(value, str):
        return value
    return None


class Parser(BaseParser, ArticleParser):
    """Parses HtmlSnapshots from ultimahora.com into Article entities.

    Fields come from the page's NewsArticle JSON-LD when present, with
    OpenGraph/article meta tags as fallback; the body is the article
    container's paragraph text and the section falls back to the
    breadcrumb trail.

    decode() bundles the parsed soup, the JSON-LD Article block, and the
    bronze source_url so the eight extract_* hooks need nothing else.
    build_item drops records that lack an Article JSON-LD — the site
    serves hub/topic templates under the same hostname that are not
    individual articles and would otherwise yield all-null silver rows.
    """

    bronze_model = HtmlSnapshot
    silver_model = Article

    BODY_P_SELECTOR = ".RichTextArticleBody p, .Page-articleBody p, article p"
    BODY_CONTAINER_SELECTOR = ".RichTextArticleBody, .Page-articleBody, article"
    BREADCRUMB_SELECTOR = ".Breadcrumb a, nav.breadcrumb a"

    def decode(self, record: Base) -> dict:
        soup: BeautifulSoup = super().decode(record)
        return {
            "soup": soup,
            "source_url": record.source_url,
            "json_ld": _find_json_ld(soup),
        }

    # _find_json_ld returns {} when no NewsArticle/Article block is present;
    # those pages are hub/topic templates, not articles — skip them.
    def build_item(self, decoded: dict) -> list[dict]:
        if not decoded.get("json_ld"):
            return []
        return [decoded]

    def extract_source_url(self, item: dict) -> str:
        return item["source_url"]

    # JSON-LD headline/name, then og:title
    def extract_title(self, item: dict) -> str:
        title = item["json_ld"].get("headline") or item["json_ld"].get("name")
        if not title:
            title = _meta_content(item["soup"], "og:title")
        return (title or "").strip()

    # article container paragraphs joined with blank lines; empty body becomes None
    def extract_body(self, item: dict) -> str | None:
        paragraphs = item["soup"].select(self.BODY_P_SELECTOR)
        body = "\n\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        return body or None

    # JSON-LD `author` is dict | list[dict|str]; flatten first non-empty name
    def extract_authors(self, item: dict) -> list[str]:
        value = item["json_ld"].get("author")
        if isinstance(value, list) and value:
            first = value[0]
            name = first.get("name") if isinstance(first, dict) else str(first)
            if name:
                return [name]
            return []
        if isinstance(value, dict) and value.get("name"):
            return [value["name"]]
        return []

    # JSON-LD datePublished, then article:published_time
    def extract_published_at(self, item: dict) -> datetime | None:
        raw = item["json_ld"].get("datePublished") or _meta_content(
            item["soup"], "article:published_time"
        )
        return _parse_datetime(raw)

    # article:section meta first, then the breadcrumb trail
    def extract_section(self, item: dict) -> str | None:
        section = _meta_content(item["soup"], "article:section")
        if section:
            return section
        crumb = item["soup"].select_one(self.BREADCRUMB_SELECTOR)
        if crumb:
            return crumb.get_text(strip=True) or None
        return None

    def extract_tags(self, item: dict) -> list[str]:
        return []

    # hero (JSON-LD `image`, then og:image-if-not-logo, then first body image)
    # + body images, http only, deduped. Editorial pages drop og:image because
    # UH serves the publication logo there.
    def extract_image_urls(self, item: dict) -> list[str]:
        # collect http images inside the body, minus logos/icons/tracking pixels
        body_images: list[str] = []
        container = item["soup"].select_one(self.BODY_CONTAINER_SELECTOR)
        if container is not None:
            for img in container.select("img[src]"):
                src = img.get("src", "")
                if not src.startswith("http"):
                    continue
                if any(kw in src.lower() for kw in IMAGE_EXCLUDE):
                    continue
                if src not in body_images:
                    body_images.append(src)

        # pick a hero, refusing the publication logo and other non-editorial assets
        hero = _ld_image_url(item["json_ld"].get("image"))
        if not hero:
            og = _meta_content(item["soup"], "og:image")
            if og and not any(kw in og.lower() for kw in IMAGE_EXCLUDE):
                hero = og
        if not hero and body_images:
            hero = body_images[0]

        out: list[str] = []
        for url in (hero, *body_images):
            if url and url not in out:
                out.append(url)
        return out
