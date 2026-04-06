"""Configurable HTML cleaning and compression for snapshot storage.

Strips noise (scripts, styles, nav, footer, etc.) while preserving
elements parsers need: JSON-LD blocks, embedded data scripts, and
data attributes used for content extraction.
"""

import hashlib
import re
import zlib
from dataclasses import dataclass, field

from bs4 import BeautifulSoup, Comment

DEFAULT_STRIP_TAGS = {"style", "noscript", "svg", "iframe", "link", "nav", "header", "footer"}

DEFAULT_ALLOWED_ATTRS = frozenset({
    "id", "class", "href", "src", "alt", "content", "property", "name", "type",
})

DEFAULT_KEEP_SCRIPT_RE = re.compile(r"var\s+data\s*=\s*\{")


@dataclass
class HtmlCleaner:
    """Configurable HTML cleaner for snapshot storage."""

    strip_tags: set[str] = field(default_factory=lambda: set(DEFAULT_STRIP_TAGS))
    strip_classes: list[str] | None = None
    allowed_attrs: frozenset[str] = DEFAULT_ALLOWED_ATTRS
    keep_script_re: re.Pattern = field(default_factory=lambda: DEFAULT_KEEP_SCRIPT_RE)

    def _keep_script(self, tag) -> bool:
        if tag.get("type") == "application/ld+json":
            return True
        if tag.string and self.keep_script_re.search(tag.string):
            return True
        return False

    def clean(self, html: str) -> str:
        """Strip non-essential elements and attributes from HTML."""
        soup = BeautifulSoup(html, "lxml")

        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            comment.extract()

        if self.strip_classes:
            for cls in self.strip_classes:
                for el in soup.find_all(class_=cls):
                    el.decompose()

        for tag_name in self.strip_tags:
            for tag in soup.find_all(tag_name):
                tag.decompose()

        for script in soup.find_all("script"):
            if not self._keep_script(script):
                script.decompose()

        for tag in soup.find_all(True):
            attrs = dict(tag.attrs)
            for attr in attrs:
                if attr not in self.allowed_attrs:
                    del tag.attrs[attr]

        return str(soup)


def compress(text: str) -> bytes:
    """Compress text (HTML or JSON) for storage."""
    return zlib.compress(text.encode("utf-8"), level=6)


def decompress(blob: bytes) -> str:
    """Decompress blob from storage."""
    return zlib.decompress(blob).decode("utf-8")


def compute_content_hash(cleaned_html: str) -> str:
    """Compute SHA-256 hash of cleaned HTML for change detection."""
    return hashlib.sha256(cleaned_html.encode("utf-8")).hexdigest()
