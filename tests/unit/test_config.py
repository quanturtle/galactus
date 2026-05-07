from pathlib import Path

import pytest
import yaml

from galactus.config import HttpConfig, HttpOverride, load_config, resolve_http
from galactus.core.errors import ConfigError

REPO_ROOT = Path(__file__).resolve().parents[2]
GALACTUS_YAML = REPO_ROOT / "galactus.yaml"


def test_galactus_yaml_loads() -> None:
    config = load_config(GALACTUS_YAML)
    names = {s.name for s in config.sources}
    assert {"abc_color", "ultimahora", "biggie", "stock"}.issubset(names)


def test_each_source_has_bronze_and_silver_table() -> None:
    config = load_config(GALACTUS_YAML)
    for src in config.sources:
        assert src.bronze_table
        assert src.silver_table


def test_each_extract_source_has_module() -> None:
    config = load_config(GALACTUS_YAML)
    for src in config.sources:
        if src.extract is not None:
            assert src.extract.module
        if src.transform is not None:
            assert src.transform.module


def test_default_concurrency_is_one() -> None:
    config = load_config(GALACTUS_YAML)
    for src in config.sources:
        if src.extract is not None:
            assert src.extract.concurrency >= 1


def test_explicit_concurrency_parses(tmp_path: Path) -> None:
    config_file = tmp_path / "demo.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "database": {"dsn": "postgresql://x/y"},
                "sources": [
                    {
                        "name": "alpha",
                        "bronze_table": "bronze.x",
                        "silver_table": "silver.x",
                        "extract": {
                            "module": "pkg.alpha",
                            "scraper": "alpha",
                            "concurrency": 7,
                        },
                    }
                ],
            }
        )
    )
    config = load_config(config_file)
    assert config.sources[0].extract is not None
    assert config.sources[0].extract.concurrency == 7


def test_concurrency_zero_rejected(tmp_path: Path) -> None:
    config_file = tmp_path / "bad.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "database": {"dsn": "postgresql://x/y"},
                "sources": [
                    {
                        "name": "src",
                        "bronze_table": "bronze.x",
                        "silver_table": "silver.x",
                        "extract": {
                            "module": "pkg.x",
                            "scraper": "x",
                            "concurrency": 0,
                        },
                    }
                ],
            }
        )
    )
    with pytest.raises(ConfigError):
        load_config(config_file)


def test_source_missing_bronze_table_rejected(tmp_path: Path) -> None:
    config_file = tmp_path / "bad.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "database": {"dsn": "postgresql://x/y"},
                "sources": [{"name": "x", "silver_table": "silver.x"}],
            }
        )
    )
    with pytest.raises(ConfigError):
        load_config(config_file)


def test_resolve_http_no_override_returns_domain() -> None:
    domain = HttpConfig(timeout_seconds=15.0, user_agent="ua/1")
    assert resolve_http(domain, None) is domain


def test_resolve_http_per_key_merge() -> None:
    domain = HttpConfig(timeout_seconds=30.0, user_agent="ua/1")
    override = HttpOverride(timeout_seconds=60.0)
    merged = resolve_http(domain, override)
    assert merged.timeout_seconds == 60.0
    assert merged.user_agent == "ua/1"


def test_resolve_http_full_override() -> None:
    domain = HttpConfig(timeout_seconds=30.0, user_agent="ua/1")
    override = HttpOverride(timeout_seconds=60.0, user_agent="ua/2")
    merged = resolve_http(domain, override)
    assert merged.timeout_seconds == 60.0
    assert merged.user_agent == "ua/2"


def test_source_http_override_loads(tmp_path: Path) -> None:
    config_file = tmp_path / "demo.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "database": {"dsn": "postgresql://x/y"},
                "http": {"timeout_seconds": 30.0, "user_agent": "ua/1"},
                "sources": [
                    {
                        "name": "alpha",
                        "bronze_table": "bronze.x",
                        "silver_table": "silver.x",
                        "http": {"timeout_seconds": 90.0},
                        "extract": {"module": "pkg.x", "scraper": "x"},
                    }
                ],
            }
        )
    )
    config = load_config(config_file)
    assert config.sources[0].http is not None
    merged = resolve_http(config.http, config.sources[0].http)
    assert merged.timeout_seconds == 90.0
    assert merged.user_agent == "ua/1"
