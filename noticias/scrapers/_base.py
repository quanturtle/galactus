"""noticias domain — scraper factory invocation."""

from pathlib import Path

from galactus.parsers import ParserPolicyRegistry
from galactus.scrapers.factory import make_domain_scrapers
from noticias.parsers import DEFAULT_PARSER_KWARGS

CONFIG_DIR = Path(__file__).resolve().parent.parent / "configs"

PARSER_REGISTRY = ParserPolicyRegistry.from_configs(
    CONFIG_DIR, defaults=DEFAULT_PARSER_KWARGS,
)

ApiScraper, BfsScraper = make_domain_scrapers(
    config_dir=CONFIG_DIR,
    parser_registry=PARSER_REGISTRY,
)
