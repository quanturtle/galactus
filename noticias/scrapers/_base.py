"""Project-level base scrapers that wire up noticias-specific defaults."""

import re
from pathlib import Path

from galactus.html_cleaner import HtmlCleaner
from galactus.scrapers.api import ApiScraper as _ApiScraper
from galactus.scrapers.bfs import BfsScraper as _BfsScraper
from galactus.storage import PsycopgApiStorage, PsycopgSnapshotStorage

CONFIG_DIR = Path(__file__).resolve().parent.parent / "configs"

ALLOWED_ATTRS = frozenset({
    "id", "class", "href", "src", "alt", "content", "property",
    "name", "type", "itemprop", "datetime", "rel",
})

KEEP_SCRIPT_RE = re.compile(r"(Fusion\.globalContent|var\s+data)\s*=\s*\{")


class ApiScraper(_ApiScraper):
    def __init__(self):
        super().__init__(storage=PsycopgApiStorage(), config_dir=CONFIG_DIR)


class BfsScraper(_BfsScraper):
    def __init__(self):
        super().__init__(
            storage=PsycopgSnapshotStorage(),
            config_dir=CONFIG_DIR,
            html_cleaner=HtmlCleaner(
                allowed_attrs=ALLOWED_ATTRS,
                keep_script_re=KEEP_SCRIPT_RE,
            ),
            use_content_hash=True,
        )
