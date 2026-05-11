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
    """Parses HtmlSnapshots from ultimahora.com into Article entities.

    One article page -> one Article. Fields come from the page's NewsArticle
    JSON-LD when present, with OpenGraph/article meta tags as fallback; the body
    is the article container's paragraph text and the section falls back to the
    breadcrumb trail. Pages with no resolvable title are skipped (silver.articles
    requires one).
    """

    bronze_model = HtmlSnapshot
    silver_model = Article

    BODY_P_SELECTOR = ".RichTextArticleBody p, .Page-articleBody p, article p"
    BODY_CONTAINER_SELECTOR = ".RichTextArticleBody, .Page-articleBody, article"
    BREADCRUMB_SELECTOR = ".Breadcrumb a, nav.breadcrumb a"

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
        image_url: str | None = None
        if json_ld:
            title = json_ld.get("headline") or json_ld.get("name")
            published_at = json_ld.get("datePublished")
            ld_author = json_ld.get("author")
            if isinstance(ld_author, list) and ld_author:
                first = ld_author[0]
                name = first.get("name") if isinstance(first, dict) else str(first)
                if name:
                    authors = [name]
            elif isinstance(ld_author, dict) and ld_author.get("name"):
                authors = [ld_author["name"]]
            ld_image = json_ld.get("image")
            if isinstance(ld_image, list) and ld_image:
                first = ld_image[0]
                image_url = first.get("url") if isinstance(first, dict) else first
            elif isinstance(ld_image, dict):
                image_url = ld_image.get("url")
            elif isinstance(ld_image, str):
                image_url = ld_image

        # fall back to OpenGraph / article meta tags
        title = title or self._meta(soup, "og:title")
        published_at = published_at or self._meta(soup, "article:published_time")
        image_url = image_url or self._meta(soup, "og:image")

        # section: article:section meta, else the breadcrumb trail
        section = self._meta(soup, "article:section")
        if not section:
            crumb = soup.select_one(self.BREADCRUMB_SELECTOR)
            if crumb:
                section = crumb.get_text(strip=True) or None

        # body: the article container's paragraph text
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
                image_urls=image_urls,
            )
        ]
