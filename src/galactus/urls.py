"""URL utilities for web scraping."""

import re
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

SKIP_EXTENSIONS = frozenset({
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
    ".css", ".js", ".zip", ".mp4", ".mp3", ".ico", ".woff", ".woff2",
})
SKIP_PREFIXES = ("mailto:", "tel:", "javascript:", "data:", "whatsapp:", "#")


def normalize(url: str, strip_path_prefixes: list[str] | None = None) -> str:
    """Normalize a URL by sorting query params and stripping trailing slashes."""
    parsed = urlparse(url)
    scheme = "https" if parsed.scheme in ("http", "https") else parsed.scheme
    netloc = parsed.netloc
    if not netloc.startswith("www."):
        netloc = "www." + netloc
    query = urlencode(
        sorted(parse_qs(parsed.query, keep_blank_values=True).items()),
        doseq=True,
    )
    path = parsed.path.rstrip("/") or "/"
    if strip_path_prefixes:
        for prefix in strip_path_prefixes:
            if path.startswith(prefix):
                path = path[len(prefix) - 1:]  # keep leading /
                break
    return urlunparse((scheme, netloc, path, parsed.params, query, ""))


def extract_same_domain_links(html: str, page_url: str, home_domain: str) -> list[str]:
    """Extract all same-domain links from an HTML page."""
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = str(a["href"]).strip()
        if any(href.startswith(p) for p in SKIP_PREFIXES):
            continue
        absolute = urljoin(page_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            continue
        if parsed.netloc != home_domain:
            continue
        ext = parsed.path.rsplit(".", 1)[-1] if "." in parsed.path.split("/")[-1] else ""
        if f".{ext}" in SKIP_EXTENSIONS:
            continue
        # Skip URLs with consecutive duplicate path segments (broken relative links)
        parts = parsed.path.strip("/").split("/")
        if any(x == y for x, y in zip(parts, parts[1:])):
            continue
        if absolute not in seen:
            seen.add(absolute)
            urls.append(absolute)
    return urls


def should_ignore(url: str, patterns: list[re.Pattern]) -> bool:
    """Check if a URL matches any ignore pattern."""
    return any(p.search(url) for p in patterns)
