import importlib
import pkgutil
from pathlib import Path

from noticias.scrapers._base import ApiScraper, BfsScraper


def _discover_scrapers() -> dict[str, type]:
    scrapers = {}
    package_dir = Path(__file__).parent
    for _, name, _ in pkgutil.iter_modules([str(package_dir)]):
        if name.startswith("_"):
            continue
        module = importlib.import_module(f"{__package__}.{name}")
        for attr in vars(module).values():
            if (
                isinstance(attr, type)
                and issubclass(attr, (ApiScraper, BfsScraper))
                and attr not in (ApiScraper, BfsScraper)
                and hasattr(attr, "source")
            ):
                scrapers[attr.source] = attr
    return scrapers


SCRAPERS = _discover_scrapers()
