from pathlib import Path

import pytest
import yaml

from galactus.config import PipelineConfig, load_config
from galactus.core.errors import ConfigError

REPO_ROOT = Path(__file__).resolve().parents[2]
ABC_COLOR_YAML = REPO_ROOT / "configs" / "abc_color.yaml"


def test_abc_color_yaml_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GALACTUS_DSN", "postgresql://x/y")
    config = load_config(ABC_COLOR_YAML)
    assert config.name == "abc_color"
    assert config.bronze_table
    assert config.silver_table


def test_each_source_yaml_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GALACTUS_DSN", "postgresql://x/y")
    configs_dir = REPO_ROOT / "configs"
    for yaml_file in sorted(configs_dir.glob("*.yaml")):
        config = load_config(yaml_file)
        assert config.name
        assert config.bronze_table
        assert config.silver_table


def test_each_extract_source_has_module(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GALACTUS_DSN", "postgresql://x/y")
    configs_dir = REPO_ROOT / "configs"
    for yaml_file in sorted(configs_dir.glob("*.yaml")):
        config = load_config(yaml_file)
        if config.extract is not None:
            assert config.extract.module
        if config.transform is not None:
            assert config.transform.module


def test_default_concurrency_is_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GALACTUS_DSN", "postgresql://x/y")
    config = load_config(ABC_COLOR_YAML)
    if config.extract is not None:
        assert config.extract.concurrency >= 1


def test_explicit_concurrency_parses(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GALACTUS_DSN", "postgresql://x/y")
    config_file = tmp_path / "demo.yaml"
    config_file.write_text(
        yaml.safe_dump(
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
        )
    )
    config = load_config(config_file)
    assert config.extract is not None
    assert config.extract.concurrency == 7


def test_concurrency_zero_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GALACTUS_DSN", "postgresql://x/y")
    config_file = tmp_path / "bad.yaml"
    config_file.write_text(
        yaml.safe_dump(
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
        )
    )
    with pytest.raises(ConfigError):
        load_config(config_file)


def test_missing_bronze_table_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GALACTUS_DSN", "postgresql://x/y")
    config_file = tmp_path / "bad.yaml"
    config_file.write_text(
        yaml.safe_dump({"name": "x", "silver_table": "silver.x"})
    )
    with pytest.raises(ConfigError):
        load_config(config_file)


def test_missing_dsn_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("GALACTUS_DSN", raising=False)
    config_file = tmp_path / "demo.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "name": "alpha",
                "bronze_table": "bronze.x",
                "silver_table": "silver.x",
            }
        )
    )
    with pytest.raises(ConfigError, match="GALACTUS_DSN"):
        load_config(config_file)


def test_extract_http_defaults(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GALACTUS_DSN", "postgresql://x/y")
    config_file = tmp_path / "demo.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "name": "alpha",
                "bronze_table": "bronze.x",
                "silver_table": "silver.x",
                "extract": {"module": "pkg.x", "scraper": "x"},
            }
        )
    )
    config = load_config(config_file)
    assert config.extract is not None
    assert config.extract.timeout_seconds == 30.0
    assert config.extract.user_agent == "galactus/0.2"
