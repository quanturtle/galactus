import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.article import Article
from sql.base import Base

SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

# "Mayo 14, 2026 11:20 a. m." — month name + day, year, optional time with a. m. / p. m.
DATE_PATTERN = re.compile(
    r"\b(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)"
    r"\s+(\d{1,2}),\s+(\d{4})"
    r"(?:\s+(\d{1,2}):(\d{2})(?:\s*(a|p)\.?\s*m\.?)?)?",
    re.IGNORECASE,
)

# strip the trailing " | NPY" / " - Noticias..." suffix from <title> when used as fallback title
TITLE_SUFFIX_PATTERN = re.compile(r"\s*[|\-–—]\s*(NPY|Noticias del Paraguay).*$", re.IGNORECASE)

# selectors to try in order for the article body container
BODY_SELECTORS = (
    "article",
    ".article-body",
    ".story-body",
    ".rich-text-body",
    "main",
)

# substrings that mark a non-editorial image (logos, icons, tracking pixels…)
IMAGE_EXCLUDE = frozenset(
    {"logo", "icon", "avatar", "emoji", "gravatar", "sprite", "pixel", "tracking", "badge"}
)


class Parser(BaseParser):
    """Parses HtmlSnapshots from npy.com.py into Article entities.

    NPY runs Brightspot and emits no JSON-LD or OpenGraph. Title falls back
    through ``<h1>``, ``<title>`` (with site suffix stripped); the body is
    paragraph text from the first article-shaped container; the section is
    the first path segment after ``/noticias/``; the published timestamp is
    parsed from a visible Spanish-language date string ("Mayo 14, 2026
    11:20 a. m."). Pages without a resolvable title are skipped.
    """

    bronze_model = HtmlSnapshot
    silver_model = Article

    # h1 first, then <title> with the site suffix stripped
    def _title(self, soup: BeautifulSoup) -> str | None:
        h1 = soup.find("h1")
        if h1:
            text = h1.get_text(" ", strip=True)
            if text:
                return text
        if soup.title and soup.title.string:
            cleaned = TITLE_SUFFIX_PATTERN.sub("", soup.title.string).strip()
            if cleaned:
                return cleaned
        return None

    # first article-shaped container's paragraphs, joined with blank lines
    def _body(self, soup: BeautifulSoup) -> str | None:
        for selector in BODY_SELECTORS:
            container = soup.select_one(selector)
            if container is None:
                continue
            chunks = [p.get_text(" ", strip=True) for p in container.find_all("p")]
            text = "\n\n".join(c for c in chunks if c)
            if text:
                return text
        return None

    # section = first path segment after /noticias/
    def _section_from_url(self, url: str) -> str | None:
        parts = [p for p in urlparse(url).path.split("/") if p]
        if len(parts) >= 2 and parts[0] == "noticias":
            return parts[1].replace("-", " ").title() or None
        return None

    # parse "Mayo 14, 2026 11:20 a. m." style strings; None on no match
    def _parse_spanish_datetime(self, text: str) -> datetime | None:
        match = DATE_PATTERN.search(text)
        if not match:
            return None
        month = SPANISH_MONTHS.get(match.group(1).lower())
        if month is None:
            return None
        day = int(match.group(2))
        year = int(match.group(3))
        hour = int(match.group(4)) if match.group(4) else 0
        minute = int(match.group(5)) if match.group(5) else 0
        meridiem = (match.group(6) or "").lower()
        if meridiem == "p" and hour < 12:
            hour += 12
        elif meridiem == "a" and hour == 12:
            hour = 0
        try:
            return datetime(year, month, day, hour, minute)
        except ValueError:
            return None

    # http image URLs anywhere in the page that look editorial (Brightspot CDN), deduped
    def _image_urls(self, soup: BeautifulSoup) -> list[str]:
        out: list[str] = []
        for img in soup.find_all("img", src=True):
            src = img.get("src", "")
            if not src.startswith("http"):
                continue
            if any(kw in src.lower() for kw in IMAGE_EXCLUDE):
                continue
            if src not in out:
                out.append(src)
        return out

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        soup: BeautifulSoup = decoded

        # title is required; skip pages without one
        title = self._title(soup)
        if not title:
            return []

        body = self._body(soup)
        section = self._section_from_url(record.source_url)
        published_at = self._parse_spanish_datetime(soup.get_text(" ", strip=True))
        image_urls = self._image_urls(soup)

        return [
            Article(
                source=self.source,
                source_url=record.source_url,
                title=title,
                body=body,
                authors=[],
                published_at=published_at,
                section=section,
                image_urls=image_urls,
            )
        ]
