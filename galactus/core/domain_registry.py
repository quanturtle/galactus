import importlib
from dataclasses import dataclass

from pydantic import BaseModel


@dataclass(frozen=True, slots=True)
class DomainSpec:
    """Static description of one domain — its entity model, target table, and plugin modules.

    `scraper_modules` and `parser_modules` are full dotted import paths. They are
    listed as strings (not imported eagerly) so the registry stays free of cycles
    and the pipeline can load only what a given config needs.
    """

    name: str
    entity_model: type[BaseModel]
    silver_table: str
    scraper_modules: tuple[str, ...]
    parser_modules: tuple[str, ...]


_REGISTRY: dict[str, DomainSpec] = {}


def register_domain(
    name: str,
    *,
    entity_model: type[BaseModel],
    silver_table: str,
    scraper_modules: tuple[str, ...],
    parser_modules: tuple[str, ...],
) -> None:
    """Record a domain's metadata. Idempotent if called again with the same spec."""
    spec = DomainSpec(
        name=name,
        entity_model=entity_model,
        silver_table=silver_table,
        scraper_modules=scraper_modules,
        parser_modules=parser_modules,
    )
    existing = _REGISTRY.get(name)
    if existing is not None and existing != spec:
        raise ValueError(f"domain {name!r} already registered with a different spec")
    _REGISTRY[name] = spec
    return


def get_domain(name: str) -> DomainSpec:
    try:
        return _REGISTRY[name]
    except KeyError as exc:
        known = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise KeyError(f"unknown domain {name!r}; registered: {known}") from exc


def registered_domains() -> list[str]:
    return sorted(_REGISTRY)


def import_domain(name: str) -> DomainSpec:
    """Populate the registry for `name`, then load every plugin module it declares.

    Importing `galactus.transform.parsers.<name>` runs `register_domain(...)` for
    that domain. The returned DomainSpec lists each scraper and parser module by
    dotted path; importing them fires the @register_scraper / @register_parser
    decorators so the plugin registries are populated.
    """
    importlib.import_module(f"galactus.transform.parsers.{name}")
    spec = get_domain(name)
    for module_path in spec.scraper_modules:
        importlib.import_module(module_path)
    for module_path in spec.parser_modules:
        importlib.import_module(module_path)
    return spec
