"""Project-level base scrapers that wire up noticias-specific defaults."""

import re
from pathlib import Path

from galactus.scrapers.factory import make_domain_scrapers

CONFIG_DIR = Path(__file__).resolve().parent.parent / "configs"

ALLOWED_ATTRS = frozenset({
    "id", "class", "href", "src", "alt", "content", "property",
    "name", "type", "itemprop", "datetime", "rel",
})

KEEP_SCRIPT_RE = re.compile(r"(Fusion\.globalContent|var\s+data)\s*=\s*\{")

ApiScraper, BfsScraper = make_domain_scrapers(
    config_dir=CONFIG_DIR,
    allowed_attrs=ALLOWED_ATTRS,
    keep_script_re=KEEP_SCRIPT_RE,
)
