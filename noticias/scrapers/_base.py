"""Project-level base scrapers that wire up noticias-specific defaults."""

import re
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from the_scraper.html_cleaner import HtmlCleaner
from the_scraper.scrapers.api import ApiScraper as _ApiScraper
from the_scraper.scrapers.bfs import BfsScraper as _BfsScraper
from noticias.storage import SQLAlchemyApiStorage, SQLAlchemySnapshotStorage

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "scrapers"

ALLOWED_ATTRS = frozenset({
    "id", "class", "href", "src", "alt", "content", "property",
    "name", "type", "itemprop", "datetime", "rel",
})

KEEP_SCRIPT_RE = re.compile(r"(Fusion\.globalContent|var\s+data)\s*=\s*\{")


class ApiScraper(_ApiScraper):
    def __init__(self, session: AsyncSession):
        storage = SQLAlchemyApiStorage(session)
        super().__init__(storage=storage, config_dir=CONFIG_DIR)


class BfsScraper(_BfsScraper):
    def __init__(self, session: AsyncSession):
        storage = SQLAlchemySnapshotStorage(session)
        super().__init__(
            storage=storage,
            config_dir=CONFIG_DIR,
            html_cleaner=HtmlCleaner(
                allowed_attrs=ALLOWED_ATTRS,
                keep_script_re=KEEP_SCRIPT_RE,
            ),
        )
