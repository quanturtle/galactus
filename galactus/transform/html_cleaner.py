"""HTML parsing, compression helpers, and per-source filter configuration."""

import hashlib
import zlib
from typing import Any

from bs4 import BeautifulSoup, Comment


class HtmlParser:
    """Applies ordered blocklist filter passes to an HTML document.

    Filters run in declaration order:
      1. strip HTML comments
      2. blocklist_tags       — decompose these tags (and their subtree)
      3. blocklist_attributes — delete these attributes from every remaining tag
    """

    def __init__(
        self,
        blocklist_tags: tuple[str, ...] = (),
        blocklist_attributes: tuple[str, ...] = (),
    ) -> None:
        self.blocklist_tags = blocklist_tags
        self.blocklist_attributes = blocklist_attributes

    @classmethod
    def from_options(cls, options: dict[str, Any]) -> "HtmlParser":
        """Build from a raw options dict (transform.options in YAML)."""
        return cls(
            blocklist_tags=tuple(options.get("blocklist_tags", ())),
            blocklist_attributes=tuple(options.get("blocklist_attributes", ())),
        )

    def _strip_comments(self, soup: BeautifulSoup) -> None:
        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            comment.extract()

    def parse(self, html: str) -> BeautifulSoup:
        """Apply all filters and return a BeautifulSoup tree."""
        soup = BeautifulSoup(html, "lxml")

        # strip comments, then apply blocklists in declaration order
        self._strip_comments(soup)

        for tag_name in self.blocklist_tags:
            for tag in soup.find_all(tag_name):
                tag.decompose()

        for tag in soup.find_all(True):
            for attr in self.blocklist_attributes:
                tag.attrs.pop(attr, None)

        return soup

    def clean(self, html: str) -> str:
        """Apply all filters and return a cleaned HTML string."""
        return str(self.parse(html))


def compress(text: str) -> bytes:
    """zlib-compress a UTF-8 string for BYTEA storage."""
    return zlib.compress(text.encode("utf-8"), level=6)


def decompress(blob: bytes) -> str:
    """Decompress a zlib blob back to a UTF-8 string."""
    return zlib.decompress(blob).decode("utf-8")


def compute_content_hash(cleaned_html: str) -> str:
    """SHA-256 of cleaned HTML for change-detection dedup."""
    return hashlib.sha256(cleaned_html.encode("utf-8")).hexdigest()
