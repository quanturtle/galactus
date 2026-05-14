from datetime import datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.article import Article
from sql.base import Base


class Parser(BaseParser):
    """Parses ApiSnapshots from latribuna.com.py into Article entities.

    The bronze body is an Arc Publishing PF ``story-feed-query`` page; each
    ``content_elements`` entry is a story object whose body lives in its own
    nested ``content_elements``. Stories without a resolvable headline are
    skipped (silver.articles requires one).
    """

    bronze_model = ApiSnapshot
    silver_model = Article

    SITE_BASE = "https://www.latribuna.com.py"

    # canonical_url is a site-relative path; join against SITE_BASE
    def _source_url(self, story: dict) -> str:
        path = (story.get("canonical_url") or story.get("website_url") or "").strip()
        if not path:
            return ""
        return urljoin(self.SITE_BASE, path)

    # body = paragraphs from inner text/raw_html content_elements joined with blank lines
    def _body(self, story: dict) -> str | None:
        chunks: list[str] = []
        for el in story.get("content_elements") or []:
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
    def _authors(self, story: dict) -> list[str]:
        credits = story.get("credits") or {}
        out: list[str] = []
        for person in credits.get("by") or []:
            if not isinstance(person, dict):
                continue
            name = (person.get("name") or "").strip()
            if name:
                out.append(name)
        return out

    # publish_date is the Arc canonical timestamp; first_publish_date / display_date are fallbacks
    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return dateparser.parse(value)
        except (ValueError, OverflowError, TypeError):
            return None

    # section: prefer primary_section.name, then first sections[].name; tags from taxonomy.tags[]
    def _section_and_tags(self, story: dict) -> tuple[str | None, list[str]]:
        taxonomy = story.get("taxonomy") or {}
        section: str | None = None
        primary = taxonomy.get("primary_section")
        if isinstance(primary, dict):
            section = (primary.get("name") or "").strip() or None
        if not section:
            for entry in taxonomy.get("sections") or []:
                if isinstance(entry, dict):
                    name = (entry.get("name") or "").strip()
                    if name:
                        section = name
                        break
        tags: list[str] = []
        for tag in taxonomy.get("tags") or []:
            if not isinstance(tag, dict):
                continue
            text = (tag.get("text") or tag.get("slug") or "").strip()
            if text:
                tags.append(text)
        return section, tags

    # promo image first, then inner content_elements images, deduped
    def _image_urls(self, story: dict) -> list[str]:
        out: list[str] = []
        promo = (story.get("promo_items") or {}).get("basic")
        if isinstance(promo, dict) and promo.get("type") == "image":
            url = (promo.get("url") or "").strip()
            if url:
                out.append(url)
        for el in story.get("content_elements") or []:
            if not isinstance(el, dict) or el.get("type") != "image":
                continue
            url = (el.get("url") or "").strip()
            if url and url not in out:
                out.append(url)
        return out

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        # Arc PF feeds nest stories under content_elements
        stories = decoded.get("content_elements") if isinstance(decoded, dict) else None
        if not isinstance(stories, list):
            return []

        articles: list[Base] = []
        for story in stories:
            if not isinstance(story, dict):
                continue

            # headline is required; skip stories without one
            headlines = story.get("headlines") or {}
            title = (headlines.get("basic") or "").strip()
            if not title:
                continue

            section, tags = self._section_and_tags(story)
            published_at = self._parse_datetime(
                story.get("publish_date")
                or story.get("first_publish_date")
                or story.get("display_date")
            )

            articles.append(
                Article(
                    source=self.source,
                    source_url=self._source_url(story),
                    title=title,
                    body=self._body(story),
                    authors=self._authors(story),
                    published_at=published_at,
                    section=section,
                    tags=tags,
                    image_urls=self._image_urls(story),
                )
            )
        return articles
