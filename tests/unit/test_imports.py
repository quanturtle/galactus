"""Smoke tests: import graph compiles and core/ has no other-layer imports."""

import ast
import importlib
import pkgutil
from pathlib import Path

import galactus.core


def walk_imports(package_root: Path) -> list[tuple[Path, str]]:
    # collect (file, imported_module) pairs
    pairs = []
    for py in package_root.rglob("*.py"):
        tree = ast.parse(py.read_text(), filename=str(py))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    pairs.append((py, alias.name))
            elif isinstance(node, ast.ImportFrom) and node.module:
                pairs.append((py, node.module))
    return pairs


def test_core_has_no_forbidden_imports() -> None:
    # core/ must not import any other galactus subpackage
    forbidden_prefixes = (
        "galactus.infra",
        "galactus.extract",
        "galactus.transform",
        "galactus.load",
        "galactus.pipeline",
        "galactus.config",
    )
    pairs = walk_imports(Path(galactus.core.__file__).parent)
    bad = [(f, m) for f, m in pairs if m.startswith(forbidden_prefixes)]
    assert not bad, f"core/ has forbidden imports: {bad}"


def test_domain_imports_register_plugins() -> None:
    # importing the per-domain scraper/parser packages fires the registration decorators
    from galactus.extract.registry import registered_scrapers
    from galactus.transform.registry import registered_parsers

    importlib.import_module("galactus.extract.scrapers.noticias")
    importlib.import_module("galactus.extract.scrapers.supermercados")
    importlib.import_module("galactus.transform.parsers.noticias")
    importlib.import_module("galactus.transform.parsers.supermercados")

    scrapers = set(registered_scrapers())
    parsers = set(registered_parsers())

    assert {"ultimahora", "abc_color", "biggie", "stock"}.issubset(scrapers)
    assert {"ultimahora", "abc_color", "biggie", "stock"}.issubset(parsers)


def test_all_subpackages_import() -> None:
    # every submodule under galactus.* (except the entrypoint pipeline.py)
    # imports cleanly
    import galactus

    skip = {"galactus.pipeline"}
    failed: list[tuple[str, str]] = []
    for module_info in pkgutil.walk_packages(galactus.__path__, prefix="galactus."):
        if module_info.name in skip:
            continue
        try:
            importlib.import_module(module_info.name)
        except Exception as exc:
            failed.append((module_info.name, repr(exc)))
    assert not failed, f"failed imports: {failed}"
