"""Shared HTML parsing helpers used by domain parsers.

Concrete logic (json-ld extraction, meta tags, body image extraction, html cleaner)
will be ported from v1's src/galactus/parsing.py and src/galactus/html_cleaner.py.
"""

from typing import Any


def extract_json_ld(html: str) -> list[dict[str, Any]]:
    """Return all <script type="application/ld+json"> payloads parsed as dicts."""
    raise NotImplementedError


def extract_meta_tags(html: str) -> dict[str, str]:
    """Return name/property -> content for <meta> tags."""
    raise NotImplementedError


def extract_body_images(html: str) -> list[str]:
    """Return absolute image URLs found in the document body."""
    raise NotImplementedError


def clean_html(html: str) -> str:
    """Strip nav/scripts/style and return reading-mode text-bearing html."""
    raise NotImplementedError
