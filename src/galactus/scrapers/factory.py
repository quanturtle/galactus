"""Factory for domain-specific scraper base classes.

Binds a config_dir + caller-provided parser registry (HTML preservation
policies) + psycopg storage into closed-over ApiScraper / BfsScraper
subclasses. Per-source scrapers subclass one of these. Image download is
wired separately by ``make_domain()`` — not this factory.
"""

from pathlib import Path

from galactus.parsers import ParserPolicyRegistry
from galactus.scrapers.api import ApiScraper as _ApiScraper
from galactus.scrapers.bfs import BfsScraper as _BfsScraper
from galactus.storage import PsycopgApiStorage, PsycopgSnapshotStorage


def make_domain_scrapers(
    *,
    config_dir: Path | str,
    parser_registry: ParserPolicyRegistry,
    batch_size: int | None = None,
    use_content_hash: bool = True,
) -> tuple[type[_ApiScraper], type[_BfsScraper]]:
    config_dir = Path(config_dir)

    bfs_kwargs: dict = {
        "config_dir": config_dir,
        "use_content_hash": use_content_hash,
        "parser_registry": parser_registry,
    }
    if batch_size is not None:
        bfs_kwargs["batch_size"] = batch_size

    class ApiScraper(_ApiScraper):
        def __init__(self):
            super().__init__(storage=PsycopgApiStorage(), config_dir=config_dir)

    class BfsScraper(_BfsScraper):
        def __init__(self):
            super().__init__(storage=PsycopgSnapshotStorage(), **bfs_kwargs)

    return ApiScraper, BfsScraper
