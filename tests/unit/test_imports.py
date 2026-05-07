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
        "galactus.cli",
        "galactus.config",
    )
    pairs = walk_imports(Path(galactus.core.__file__).parent)
    bad = [(f, m) for f, m in pairs if m.startswith(forbidden_prefixes)]
    assert not bad, f"core/ has forbidden imports: {bad}"


def test_domain_imports_register_plugins() -> None:
    # the domain registry is the single entry point: importing the parsers package
    # registers the domain, and pipeline.import_domain walks the spec to load plugins
    from galactus.core.domain_registry import get_domain, import_domain, registered_domains
    from galactus.extract.registry import registered_scrapers
    from galactus.transform.registry import registered_parsers

    import_domain("noticias")
    import_domain("supermercados")

    assert {"noticias", "supermercados"}.issubset(set(registered_domains()))
    assert get_domain("noticias").silver_table == "silver.articles"
    assert get_domain("supermercados").silver_table == "silver.products"

    scrapers = set(registered_scrapers())
    parsers = set(registered_parsers())
    assert {"ultimahora", "abc_color", "biggie", "stock"}.issubset(scrapers)
    assert {"ultimahora", "abc_color", "biggie", "stock"}.issubset(parsers)


def test_all_subpackages_import() -> None:
    # every submodule under galactus.* imports cleanly without side effects
    import galactus

    failed: list[tuple[str, str]] = []
    for module_info in pkgutil.walk_packages(galactus.__path__, prefix="galactus."):
        try:
            importlib.import_module(module_info.name)
        except Exception as exc:
            failed.append((module_info.name, repr(exc)))
    assert not failed, f"failed imports: {failed}"
