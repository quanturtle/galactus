"""HtmlParser: ordered filter passes over an HTML document."""

import asyncio
from typing import Any

from bs4 import BeautifulSoup, Comment

# always-decomposed at every source; per-source blocklist_tags adds to this set
BASELINE_BLOCKLIST_TAGS: tuple[str, ...] = ("script", "style", "noscript")


class HtmlParser:
    """Builds an HTML tree once with lxml and applies ordered cleaning passes.

    parse(text) returns the BeautifulSoup tree so the scraper can reuse it
    for link extraction without a second parse. clean(soup) is async — it
    offloads the filter passes to a worker thread so the asyncio loop is not
    blocked while CPU-heavy cleaning runs:

      1. strip HTML comments
      2. blocklist_tags       — decompose these tags (and their subtree)
      3. blocklist_attributes — delete these attributes from every remaining tag

    blocklist_tags is the union of BASELINE_BLOCKLIST_TAGS and any per-source
    additions from config; <script type="application/ld+json"> is always
    preserved so source-specific parsers can read structured data from it.
    """

    def __init__(self, options: dict[str, Any]) -> None:
        extra = tuple(
            t for t in options.get("blocklist_tags", ()) if t not in BASELINE_BLOCKLIST_TAGS
        )
        self.blocklist_tags: tuple[str, ...] = BASELINE_BLOCKLIST_TAGS + extra
        self.blocklist_attributes: tuple[str, ...] = tuple(options.get("blocklist_attributes", ()))

    def parse(self, text: str) -> BeautifulSoup:
        return BeautifulSoup(text, "lxml")

    def strip_comments(self, soup: BeautifulSoup) -> None:
        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            comment.extract()

    def decompose_blocklist_tags(self, soup: BeautifulSoup) -> None:
        for tag_name in self.blocklist_tags:
            for tag in soup.find_all(tag_name):
                if tag_name == "script" and tag.get("type") == "application/ld+json":
                    continue
                tag.decompose()

    def strip_blocklist_attributes(self, soup: BeautifulSoup) -> None:
        for tag in soup.find_all(True):
            for attr in self.blocklist_attributes:
                tag.attrs.pop(attr, None)

    def clean_sync(self, soup: BeautifulSoup) -> str:
        self.strip_comments(soup)
        self.decompose_blocklist_tags(soup)
        self.strip_blocklist_attributes(soup)
        return str(soup)

    async def clean(self, soup: BeautifulSoup) -> str:
        """Run the filter passes off the event loop and return cleaned HTML."""
        return await asyncio.to_thread(self.clean_sync, soup)
