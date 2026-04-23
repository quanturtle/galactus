"""Generic scraper + parser discovery for domain packages."""

import importlib
import inspect
import pkgutil
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Callable


def discover_scrapers(
    package: ModuleType, base_classes: tuple[type, ...]
) -> dict[str, type]:
    scrapers: dict[str, type] = {}
    package_dir = Path(package.__file__).parent
    for _, name, _ in pkgutil.iter_modules([str(package_dir)]):
        if name.startswith("_"):
            continue
        module = importlib.import_module(f"{package.__name__}.{name}")
        for attr in vars(module).values():
            if (
                isinstance(attr, type)
                and issubclass(attr, base_classes)
                and attr not in base_classes
                and hasattr(attr, "source")
            ):
                scrapers[attr.source] = attr
    return scrapers


def discover_parsers(
    package: ModuleType,
) -> tuple[dict[str, Callable], dict[str, Callable]]:
    """Return ``(html_parsers, api_parsers)`` by scanning *package*.

    A module is registered if it exposes a ``parse`` callable. Classification:
      - explicit: module sets ``PARSER_KIND = "html"`` or ``"api"``;
      - fallback: ``parse`` signature with >= 2 params → html, else api.
    """
    html_parsers: dict[str, Callable] = {}
    api_parsers: dict[str, Callable] = {}
    package_dir = Path(package.__file__).parent
    for _, name, _ in pkgutil.iter_modules([str(package_dir)]):
        if name.startswith("_"):
            continue
        module = importlib.import_module(f"{package.__name__}.{name}")
        parse_fn = getattr(module, "parse", None)
        if parse_fn is None:
            continue
        source = getattr(module, "SOURCE", name)
        kind = getattr(module, "PARSER_KIND", None)
        if kind is None:
            kind = "html" if len(inspect.signature(parse_fn).parameters) >= 2 else "api"
        if kind == "html":
            html_parsers[source] = parse_fn
        elif kind == "api":
            api_parsers[source] = parse_fn
        else:
            raise ValueError(f"{module.__name__}: invalid PARSER_KIND={kind!r}")
    return html_parsers, api_parsers


@dataclass
class ParserRegistry:
    html: dict[str, Callable]
    api: dict[str, Callable]

    @classmethod
    def from_package(cls, package: ModuleType) -> "ParserRegistry":
        return cls(*discover_parsers(package))

    def parse_snapshot(self, source: str, html: str, url: str) -> dict | None:
        fn = self.html.get(source)
        if fn is None:
            raise ValueError(f"No HTML parser registered for source: {source}")
        return fn(html, url)

    def parse_api_response(self, source: str, response_text: str) -> list[dict]:
        fn = self.api.get(source)
        if fn is None:
            raise ValueError(f"No API parser registered for source: {source}")
        return fn(response_text)
