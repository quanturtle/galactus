"""Two-stage HTML cleaning and compression for snapshot storage.

Stage 1 (basic filter) strips site-agnostic noise every HTML source has:
comments, non-whitelisted scripts, a baseline set of tags, and any
attribute not on the allow list.

Stage 2 (configured filter) layers per-source strip rules on top:
extra tags and CSS classes declared in the source's YAML config.
"""

import hashlib
import re
import zlib
from dataclasses import dataclass, field

from bs4 import BeautifulSoup, Comment

DEFAULT_STRIP_TAGS = frozenset({"style", "noscript", "svg", "iframe", "link", "nav", "header", "footer"})

DEFAULT_ALLOWED_ATTRS = frozenset({
    "id", "class", "href", "src", "alt", "content", "property", "name", "type",
})

DEFAULT_KEEP_SCRIPT_RE = re.compile(r"var\s+data\s*=\s*\{")


@dataclass
class HtmlCleaner:
    """Two-stage HTML cleaner.

    The basic filter (always applied) uses DEFAULT_STRIP_TAGS plus the
    allowed_attrs and keep_script_re wired by the project-level scraper.
    The configured filter layers per-source extras on top.
    """

    allowed_attrs: frozenset[str] = DEFAULT_ALLOWED_ATTRS
    keep_script_re: re.Pattern = field(default_factory=lambda: DEFAULT_KEEP_SCRIPT_RE)
    extra_strip_tags: set[str] = field(default_factory=set)
    extra_strip_classes: list[str] = field(default_factory=list)

    def _keep_script(self, tag) -> bool:
        if tag.get("type") == "application/ld+json":
            return True
        if tag.string and self.keep_script_re.search(tag.string):
            return True
        return False

    def _apply_basic_filter(self, soup: BeautifulSoup) -> None:
        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            comment.extract()

        for tag_name in DEFAULT_STRIP_TAGS:
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

    def _apply_configured_filter(self, soup: BeautifulSoup) -> None:
        for cls in self.extra_strip_classes:
            for el in soup.find_all(class_=cls):
                el.decompose()

        for tag_name in self.extra_strip_tags:
            for tag in soup.find_all(tag_name):
                tag.decompose()

    def clean(self, html: str) -> str:
        soup = BeautifulSoup(html, "lxml")
        self._apply_basic_filter(soup)
        self._apply_configured_filter(soup)
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
