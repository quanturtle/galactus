from datetime import datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from galactus.transform.article_parser import ArticleParser
from galactus.transform.base_parser import BaseParser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.article import Article


# parse the Arc canonical timestamp (publish_date / first_publish_date / display_date)
def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return dateparser.parse(value)
    except (ValueError, OverflowError, TypeError):
        return None


class Parser(BaseParser, ArticleParser):
    """Parses ApiSnapshots from abc.com.py into Article entities.

    The bronze body is an Arc Publishing PF ``sections-api`` page; each
    ``content_elements`` entry is a full story object. Story bodies live
    in the story's own nested ``content_elements`` (text + raw_html
    elements). build_item splits the outer feed into one item per
    Article.
    """

    bronze_model = ApiSnapshot
    silver_model = Article

    SITE_BASE = "https://www.abc.com.py"

    # Arc PF feeds nest stories under content_elements; tolerate empty/odd pages
    def build_item(self, decoded: Any) -> list[dict]:
        if not isinstance(decoded, dict):
            return []
        stories = decoded.get("content_elements")
        if not isinstance(stories, list):
            return []
        return [story for story in stories if isinstance(story, dict)]

    # canonical_url is a site-relative path; join against SITE_BASE
    def extract_source_url(self, item: dict) -> str:
        path = (item.get("canonical_url") or item.get("website_url") or "").strip()
        if not path:
            return ""
        return urljoin(self.SITE_BASE, path)

    def extract_title(self, item: dict) -> str:
        return ((item.get("headlines") or {}).get("basic") or "").strip()

    # body = paragraphs from inner text/raw_html content_elements joined with blank lines
    def extract_body(self, item: dict) -> str | None:
        chunks: list[str] = []
        for el in item.get("content_elements") or []:
            if not isinstance(el, dict):
                continue
            kind = el.get("type")
            if kind not in ("text", "raw_html"):
                continue
            raw = el.get("content") or ""
            text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
            if text:
                chunks.append(text)
        return "\n\n".join(chunks) or None

    # `credits.by` is a list of {name, type, ...}; keep non-empty names in order
    def extract_authors(self, item: dict) -> list[str]:
        credits = item.get("credits") or {}
        out: list[str] = []
        for person in credits.get("by") or []:
            if not isinstance(person, dict):
                continue
            name = (person.get("name") or "").strip()
            if name:
                out.append(name)
        return out

    def extract_published_at(self, item: dict) -> datetime | None:
        return _parse_datetime(
            item.get("publish_date") or item.get("first_publish_date") or item.get("display_date")
        )

    # prefer primary_section.name, then first sections[].name
    def extract_section(self, item: dict) -> str | None:
        taxonomy = item.get("taxonomy") or {}
        primary = taxonomy.get("primary_section")
        if isinstance(primary, dict):
            name = (primary.get("name") or "").strip()
            if name:
                return name
        for entry in taxonomy.get("sections") or []:
            if isinstance(entry, dict):
                name = (entry.get("name") or "").strip()
                if name:
                    return name
        return None

    # taxonomy.tags[].text (or .slug as fallback)
    def extract_tags(self, item: dict) -> list[str]:
        out: list[str] = []
        for tag in (item.get("taxonomy") or {}).get("tags") or []:
            if not isinstance(tag, dict):
                continue
            text = (tag.get("text") or tag.get("slug") or "").strip()
            if text:
                out.append(text)
        return out

    # inner content_elements images first (per-article), then promo as fallback.
    # the sections-api feed reuses the section's lead image across every story's
    # promo_items.basic.url, so promo can only be trusted when no inline image exists.
    def extract_image_urls(self, item: dict) -> list[str]:
        out: list[str] = []
        for el in item.get("content_elements") or []:
            if not isinstance(el, dict) or el.get("type") != "image":
                continue
            url = (el.get("url") or "").strip()
            if url and url not in out:
                out.append(url)
        promo = (item.get("promo_items") or {}).get("basic")
        if isinstance(promo, dict) and promo.get("type") == "image":
            url = (promo.get("url") or "").strip()
            if url and url not in out:
                out.append(url)
        return out
