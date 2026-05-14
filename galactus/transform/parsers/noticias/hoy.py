from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.article import Article
from sql.base import Base


class Parser(BaseParser):
    """Parses ApiSnapshots from hoy.com.py into Article entities.

    The bronze body is a WordPress REST ``/wp/v2/posts?_embed=true`` page —
    a JSON array. Each post becomes one Article. Author names, the featured
    image, and the section/tag terms come from the embedded objects under
    ``_embedded``. Posts without a resolvable title are skipped
    (silver.articles requires one).
    """

    bronze_model = ApiSnapshot
    silver_model = Article

    # strip HTML out of a WordPress rendered field; separator splits block-level text
    def _text(self, rendered: str, separator: str = " ") -> str:
        return BeautifulSoup(rendered, "html.parser").get_text(separator, strip=True)

    # date_gmt is naive UTC; date is the site's local time. Prefer date_gmt.
    def _parse_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return dateparser.parse(value)
        except (ValueError, OverflowError, TypeError):
            return None

    # `_embedded.author` is a list of {id, name, ...}; keep non-empty names in order
    def _authors(self, post: dict) -> list[str]:
        embedded = post.get("_embedded") or {}
        out: list[str] = []
        for author in embedded.get("author", []) or []:
            if not isinstance(author, dict):
                continue
            name = (author.get("name") or "").strip()
            if name:
                out.append(name)
        return out

    # `_embedded["wp:term"]` groups terms by taxonomy: [[categories], [tags], ...]
    def _terms(self, post: dict) -> tuple[str | None, list[str]]:
        embedded = post.get("_embedded") or {}
        section: str | None = None
        tags: list[str] = []
        for group in embedded.get("wp:term") or []:
            for term in group or []:
                if not isinstance(term, dict):
                    continue
                name = (term.get("name") or "").strip()
                if not name:
                    continue
                taxonomy = term.get("taxonomy")
                if taxonomy == "category" and section is None:
                    section = name
                elif taxonomy == "post_tag":
                    tags.append(name)
        return section, tags

    # featured-media source_urls from the embedded payload, deduped
    def _image_urls(self, post: dict) -> list[str]:
        embedded = post.get("_embedded") or {}
        out: list[str] = []
        for media in embedded.get("wp:featuredmedia", []) or []:
            if not isinstance(media, dict):
                continue
            src = (media.get("source_url") or "").strip()
            if src and src not in out:
                out.append(src)
        return out

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        # WP /posts returns a JSON array; tolerate empty/odd pages
        if not isinstance(decoded, list):
            return []

        articles: list[Base] = []
        for post in decoded:
            if not isinstance(post, dict):
                continue

            # title is required; skip posts without one
            title = self._text((post.get("title") or {}).get("rendered") or "")
            if not title:
                continue

            # body keeps paragraph breaks; empty body becomes None
            body = self._text((post.get("content") or {}).get("rendered") or "", "\n\n") or None

            section, tags = self._terms(post)
            published_at = self._parse_datetime(post.get("date_gmt") or post.get("date"))

            articles.append(
                Article(
                    source=self.source,
                    source_url=post.get("link") or "",
                    title=title,
                    body=body,
                    authors=self._authors(post),
                    published_at=published_at,
                    section=section,
                    tags=tags,
                    image_urls=self._image_urls(post),
                )
            )
        return articles
