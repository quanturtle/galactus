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
    # core/ must not import any other galactus subpackage. Stages and adapters
    # depend on core; core depends on nothing outside itself.
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


def test_plugin_modules_export_strategy_class() -> None:
    # each plugin module must expose a Scraper or Parser attribute
    scraper_mods = (
        "galactus.extract.scrapers.noticias.abc_color",
        "galactus.extract.scrapers.noticias.ultimahora",
        "galactus.extract.scrapers.supermercados.biggie",
        "galactus.extract.scrapers.supermercados.stock",
    )
    parser_mods = (
        "galactus.transform.parsers.noticias.abc_color",
        "galactus.transform.parsers.noticias.ultimahora",
        "galactus.transform.parsers.supermercados.biggie",
        "galactus.transform.parsers.supermercados.stock",
    )
    for name in scraper_mods:
        mod = importlib.import_module(name)
        assert hasattr(mod, "Scraper"), f"{name} missing Scraper"
    for name in parser_mods:
        mod = importlib.import_module(name)
        assert hasattr(mod, "Parser"), f"{name} missing Parser"


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
