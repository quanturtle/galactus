import json
from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.article import Article
from sql.base import Base

# substrings that mark a non-editorial image (logos, icons, tracking pixels…)
IMAGE_EXCLUDE = frozenset(
    {"logo", "icon", "avatar", "emoji", "gravatar", "sprite", "pixel", "tracking", "badge"}
)


class Parser(BaseParser):
    """Parses HtmlSnapshots from elnacional.com.py into Article entities.

    elnacional.com.py runs AmuraCMS and ships a Schema.org NewsArticle
    JSON-LD block on every article — that's the primary source of title,
    publish date, section, author, and hero image. OpenGraph ``og:title`` /
    ``og:image`` are kept as fallback. The body comes from the ``.content``
    container's paragraph text. Pages without a resolvable title are
    skipped (silver.articles requires one).
    """

    bronze_model = HtmlSnapshot
    silver_model = Article

    BODY_CONTAINER_SELECTOR = ".content"
    BODY_P_SELECTOR = ".content p"

    # the first <script type="application/ld+json"> describing a NewsArticle/Article
    def _json_ld(self, soup: BeautifulSoup) -> dict | None:
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
            for item in candidates:
                if isinstance(item, dict) and item.get("@type") in ("NewsArticle", "Article"):
                    return item
        return None

    # content of a <meta property=…> / <meta name=…> tag
    def _meta(self, soup: BeautifulSoup, prop: str) -> str | None:
        tag = soup.find("meta", attrs={"property": prop}) or soup.find("meta", attrs={"name": prop})
        if tag and tag.get("content"):
            return tag["content"]
        return None

    # JSON-LD `author` is dict | list[dict|str] | str; flatten to a list of names
    def _ld_authors(self, value: Any) -> list[str]:
        if isinstance(value, list):
            out: list[str] = []
            for entry in value:
                if isinstance(entry, dict) and entry.get("name"):
                    out.append(str(entry["name"]).strip())
                elif isinstance(entry, str) and entry.strip():
                    out.append(entry.strip())
            return out
        if isinstance(value, dict) and value.get("name"):
            return [str(value["name"]).strip()]
        if isinstance(value, str) and value.strip():
            return [value.strip()]
        return []

    # JSON-LD `image` is dict | list | str; pick the first url
    def _ld_image(self, value: Any) -> str | None:
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

    # JSON-LD `articleSection` may be str or list[str]; keep first non-empty
    def _ld_section(self, value: Any) -> str | None:
        if isinstance(value, list):
            for entry in value:
                if isinstance(entry, str) and entry.strip():
                    return entry.strip()
            return None
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None

    # JSON-LD `keywords` may be a comma-separated str or a list[str]
    def _ld_keywords(self, value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(t).strip() for t in value if str(t).strip()]
        if isinstance(value, str):
            return [t.strip() for t in value.split(",") if t.strip()]
        return []

    # http image URLs inside the article body, minus logos/icons/tracking pixels
    def _body_images(self, soup: BeautifulSoup) -> list[str]:
        container = soup.select_one(self.BODY_CONTAINER_SELECTOR)
        if not container:
            return []
        images: list[str] = []
        for img in container.select("img[src]"):
            src = img.get("src", "")
            if not src.startswith("http"):
                continue
            if any(kw in src.lower() for kw in IMAGE_EXCLUDE):
                continue
            if src not in images:
                images.append(src)
        return images

    # parse a published-at string; None if missing or unparseable
    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return dateparser.parse(value)
        except (ValueError, OverflowError, TypeError):
            return None

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        soup: BeautifulSoup = decoded

        # pull what we can from the JSON-LD block
        json_ld = self._json_ld(soup)
        title: str | None = None
        published_at: str | None = None
        authors: list[str] = []
        section: str | None = None
        tags: list[str] = []
        image_url: str | None = None
        if json_ld:
            title = json_ld.get("headline") or json_ld.get("name")
            published_at = json_ld.get("datePublished")
            authors = self._ld_authors(json_ld.get("author"))
            section = self._ld_section(json_ld.get("articleSection"))
            tags = self._ld_keywords(json_ld.get("keywords"))
            image_url = self._ld_image(json_ld.get("image"))

        # fall back to OpenGraph for the basics JSON-LD didn't cover
        title = title or self._meta(soup, "og:title")
        published_at = published_at or self._meta(soup, "article:published_time")
        image_url = image_url or self._meta(soup, "og:image")

        # body: the .content container's paragraph text
        paragraphs = soup.select(self.BODY_P_SELECTOR)
        body = "\n\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))

        # image_urls: hero first, then in-body images, deduped
        image_urls: list[str] = []
        for url in (image_url, *self._body_images(soup)):
            if url and url not in image_urls:
                image_urls.append(url)

        if not title:
            return []
        return [
            Article(
                source=self.source,
                source_url=record.source_url,
                title=title,
                body=body or None,
                authors=authors,
                published_at=self._parse_datetime(published_at),
                section=section,
                tags=tags,
                image_urls=image_urls,
            )
        ]
