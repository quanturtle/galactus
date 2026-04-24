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


def discover_transformers(
    package: ModuleType,
) -> tuple[dict[str, Callable], dict[str, Callable]]:
    """Return ``(html_transformers, api_transformers)`` by scanning *package*.

    A module is registered if it exposes a ``transform`` callable. Classification:
      - explicit: module sets ``TRANSFORMER_KIND = "html"`` or ``"api"``;
      - fallback: ``transform`` signature with >= 2 params → html, else api.
    """
    html_transformers: dict[str, Callable] = {}
    api_transformers: dict[str, Callable] = {}
    package_dir = Path(package.__file__).parent
    for _, name, _ in pkgutil.iter_modules([str(package_dir)]):
        if name.startswith("_"):
            continue
        module = importlib.import_module(f"{package.__name__}.{name}")
        transform_fn = getattr(module, "transform", None)
        if transform_fn is None:
            continue
        source = getattr(module, "SOURCE", name)
        kind = getattr(module, "TRANSFORMER_KIND", None)
        if kind is None:
            kind = "html" if len(inspect.signature(transform_fn).parameters) >= 2 else "api"
        if kind == "html":
            html_transformers[source] = transform_fn
        elif kind == "api":
            api_transformers[source] = transform_fn
        else:
            raise ValueError(f"{module.__name__}: invalid TRANSFORMER_KIND={kind!r}")
    return html_transformers, api_transformers


@dataclass
class TransformerRegistry:
    html: dict[str, Callable]
    api: dict[str, Callable]

    @classmethod
    def from_package(cls, package: ModuleType) -> "TransformerRegistry":
        return cls(*discover_transformers(package))

    def transform_snapshot(self, source: str, html: str, url: str) -> dict | None:
        fn = self.html.get(source)
        if fn is None:
            raise ValueError(f"No HTML transformer registered for source: {source}")
        return fn(html, url)

    def transform_api_response(self, source: str, response_text: str) -> list[dict]:
        fn = self.api.get(source)
        if fn is None:
            raise ValueError(f"No API transformer registered for source: {source}")
        return fn(response_text)
