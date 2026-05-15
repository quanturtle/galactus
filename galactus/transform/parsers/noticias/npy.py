import re
from datetime import datetime
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from galactus.transform.article_parser import ArticleParser
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


# parse "Mayo 14, 2026 11:20 a. m." style strings; None on no match
def _parse_spanish_datetime(text: str) -> datetime | None:
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


class Parser(BaseParser, ArticleParser):
    """Parses HtmlSnapshots from npy.com.py into Article entities.

    NPY runs Brightspot and emits no JSON-LD or OpenGraph. Title falls back
    through ``<h1>``, ``<title>`` (with site suffix stripped); the body is
    paragraph text from the first article-shaped container; the section is
    the first path segment after ``/noticias/``; the published timestamp is
    parsed from a visible Spanish-language date string ("Mayo 14, 2026
    11:20 a. m.").

    decode() bundles the parsed soup with the bronze source_url so the
    eight extract_* hooks need nothing else. One Article per bronze
    record, so build_item is inherited.
    """

    bronze_model = HtmlSnapshot
    silver_model = Article

    def decode(self, record: Base) -> dict:
        return {"soup": super().decode(record), "source_url": record.source_url}

    def extract_source_url(self, item: dict) -> str:
        return item["source_url"]

    # h1 first, then <title> with the site suffix stripped
    def extract_title(self, item: dict) -> str:
        soup: BeautifulSoup = item["soup"]
        h1 = soup.find("h1")
        if h1:
            text = h1.get_text(" ", strip=True)
            if text:
                return text
        if soup.title and soup.title.string:
            cleaned = TITLE_SUFFIX_PATTERN.sub("", soup.title.string).strip()
            if cleaned:
                return cleaned
        return ""

    # first article-shaped container's paragraphs, joined with blank lines
    def extract_body(self, item: dict) -> str | None:
        soup: BeautifulSoup = item["soup"]
        for selector in BODY_SELECTORS:
            container = soup.select_one(selector)
            if container is None:
                continue
            chunks = [p.get_text(" ", strip=True) for p in container.find_all("p")]
            text = "\n\n".join(c for c in chunks if c)
            if text:
                return text
        return None

    def extract_authors(self, item: dict) -> list[str]:
        return []

    # visible Spanish-language date string anywhere on the page
    def extract_published_at(self, item: dict) -> datetime | None:
        return _parse_spanish_datetime(item["soup"].get_text(" ", strip=True))

    # first path segment after /noticias/, normalized
    def extract_section(self, item: dict) -> str | None:
        parts = [p for p in urlparse(item["source_url"]).path.split("/") if p]
        if len(parts) >= 2 and parts[0] == "noticias":
            return parts[1].replace("-", " ").title() or None
        return None

    def extract_tags(self, item: dict) -> list[str]:
        return []

    # http image URLs anywhere in the page that look editorial, deduped
    def extract_image_urls(self, item: dict) -> list[str]:
        out: list[str] = []
        for img in item["soup"].find_all("img", src=True):
            src = img.get("src", "")
            if not src.startswith("http"):
                continue
            if any(kw in src.lower() for kw in IMAGE_EXCLUDE):
                continue
            if src not in out:
                out.append(src)
        return out
