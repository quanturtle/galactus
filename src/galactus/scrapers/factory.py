"""Factory for domain-specific scraper base classes.

Wires a `config_dir` + optional HtmlCleaner overrides + psycopg storage into
the framework's ApiScraper / BfsScraper, returning two subclasses the domain
can subclass further or discover directly.
"""

import re
from pathlib import Path

from galactus.html_cleaner import HtmlCleaner
from galactus.scrapers.api import ApiScraper as _ApiScraper
from galactus.scrapers.bfs import BfsScraper as _BfsScraper
from galactus.storage import PsycopgApiStorage, PsycopgSnapshotStorage


def make_domain_scrapers(
    *,
    config_dir: Path | str,
    allowed_attrs: frozenset[str] | None = None,
    keep_script_re: re.Pattern | None = None,
    batch_size: int | None = None,
    use_content_hash: bool = True,
) -> tuple[type[_ApiScraper], type[_BfsScraper]]:
    cleaner_kwargs: dict = {}
    if allowed_attrs is not None:
        cleaner_kwargs["allowed_attrs"] = allowed_attrs
    if keep_script_re is not None:
        cleaner_kwargs["keep_script_re"] = keep_script_re

    bfs_kwargs: dict = {
        "config_dir": config_dir,
        "use_content_hash": use_content_hash,
    }
    if batch_size is not None:
        bfs_kwargs["batch_size"] = batch_size

    class ApiScraper(_ApiScraper):
        def __init__(self):
            super().__init__(storage=PsycopgApiStorage(), config_dir=config_dir)

    class BfsScraper(_BfsScraper):
        def __init__(self):
            super().__init__(
                storage=PsycopgSnapshotStorage(),
                html_cleaner=HtmlCleaner(**cleaner_kwargs),
                **bfs_kwargs,
            )

    return ApiScraper, BfsScraper
