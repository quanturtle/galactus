from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from galactus.transform.article_parser import ArticleParser
from galactus.transform.base_parser import BaseParser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.article import Article


# strip HTML out of a WordPress rendered field; separator splits block-level text.
# WP content occasionally carries stray NUL bytes (embed encoder bugs); postgres
# text columns reject them, so drop them here before they reach silver.
def _text(rendered: str, separator: str = " ") -> str:
    return (
        BeautifulSoup(rendered, "html.parser").get_text(separator, strip=True).replace("\x00", "")
    )


# date_gmt is naive UTC; date is the site's local time. Prefer date_gmt.
def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return dateparser.parse(value)
    except (ValueError, OverflowError, TypeError):
        return None


class Parser(BaseParser, ArticleParser):
    """Parses ApiSnapshots from hoy.com.py into Article entities.

    The bronze body is a WordPress REST ``/wp/v2/posts?_embed=true``
    page — a JSON array. Each post becomes one Article. Author names,
    the featured image, and the section/tag terms come from the
    embedded objects under ``_embedded``. build_item splits the array
    into one item per Article.
    """

    bronze_model = ApiSnapshot
    silver_model = Article

    # WP /posts returns a JSON array; tolerate empty/odd pages
    def build_item(self, decoded: Any) -> list[dict]:
        if not isinstance(decoded, list):
            return []
        return [post for post in decoded if isinstance(post, dict)]

    def extract_source_url(self, item: dict) -> str:
        return item.get("link") or ""

    def extract_title(self, item: dict) -> str:
        return _text((item.get("title") or {}).get("rendered") or "")

    # body keeps paragraph breaks; empty body becomes None
    def extract_body(self, item: dict) -> str | None:
        return _text((item.get("content") or {}).get("rendered") or "", "\n\n") or None

    # `_embedded.author` is a list of {id, name, ...}; keep non-empty names in order
    def extract_authors(self, item: dict) -> list[str]:
        embedded = item.get("_embedded") or {}
        out: list[str] = []
        for author in embedded.get("author", []) or []:
            if not isinstance(author, dict):
                continue
            name = (author.get("name") or "").strip()
            if name:
                out.append(name)
        return out

    def extract_published_at(self, item: dict) -> datetime | None:
        return _parse_datetime(item.get("date_gmt") or item.get("date"))

    # first category term from `_embedded["wp:term"]`
    def extract_section(self, item: dict) -> str | None:
        embedded = item.get("_embedded") or {}
        for group in embedded.get("wp:term") or []:
            for term in group or []:
                if not isinstance(term, dict):
                    continue
                if term.get("taxonomy") != "category":
                    continue
                name = (term.get("name") or "").strip()
                if name:
                    return name
        return None

    # post_tag terms from `_embedded["wp:term"]`, in order
    def extract_tags(self, item: dict) -> list[str]:
        embedded = item.get("_embedded") or {}
        out: list[str] = []
        for group in embedded.get("wp:term") or []:
            for term in group or []:
                if not isinstance(term, dict):
                    continue
                if term.get("taxonomy") != "post_tag":
                    continue
                name = (term.get("name") or "").strip()
                if name:
                    out.append(name)
        return out

    # featured-media source_urls from the embedded payload, deduped
    def extract_image_urls(self, item: dict) -> list[str]:
        embedded = item.get("_embedded") or {}
        out: list[str] = []
        for media in embedded.get("wp:featuredmedia", []) or []:
            if not isinstance(media, dict):
                continue
            src = (media.get("source_url") or "").strip()
            if src and src not in out:
                out.append(src)
        return out
