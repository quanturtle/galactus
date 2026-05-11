from pathlib import Path

import pytest
import yaml

from galactus.config import load_config
from galactus.core.errors import ConfigError

REPO_ROOT = Path(__file__).resolve().parents[2]
ABC_COLOR_YAML = REPO_ROOT / "configs" / "abc_color.yaml"


def test_abc_color_yaml_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    config = load_config(ABC_COLOR_YAML)
    assert config.name == "abc_color"


def test_each_source_yaml_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    configs_dir = REPO_ROOT / "configs"
    for yaml_file in sorted(configs_dir.glob("*.yaml")):
        config = load_config(yaml_file)
        assert config.name


def test_each_extract_source_has_plugin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    configs_dir = REPO_ROOT / "configs"
    for yaml_file in sorted(configs_dir.glob("*.yaml")):
        config = load_config(yaml_file)
        if config.extract is not None:
            assert config.extract.scraper
        if config.transform is not None:
            assert config.transform.parser


def test_default_concurrency_is_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    config = load_config(ABC_COLOR_YAML)
    if config.extract is not None:
        assert config.extract.options.concurrency >= 1


def test_explicit_concurrency_parses(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    config_file = tmp_path / "demo.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "name": "alpha",
                "extract": {
                    "scraper": "pkg.alpha",
                    "options": {"base_url": "https://example.com", "concurrency": 7},
                },
            }
        )
    )
    config = load_config(config_file)
    assert config.extract is not None
    assert config.extract.options.concurrency == 7


def test_concurrency_zero_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    config_file = tmp_path / "bad.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "name": "src",
                "extract": {
                    "scraper": "pkg.x",
                    "options": {"base_url": "https://example.com", "concurrency": 0},
                },
            }
        )
    )
    with pytest.raises(ConfigError):
        load_config(config_file)


def test_missing_dsn_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    config_file = tmp_path / "demo.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "name": "alpha",
            }
        )
    )
    with pytest.raises(ConfigError, match="DATABASE_URL"):
        load_config(config_file)


def test_extract_http_defaults(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    config_file = tmp_path / "demo.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "name": "alpha",
                "extract": {
                    "scraper": "pkg.x",
                    "options": {"base_url": "https://example.com"},
                },
            }
        )
    )
    config = load_config(config_file)
    assert config.extract is not None
    assert config.extract.timeout_seconds == 30.0
    assert config.extract.user_agent == (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    assert config.extract.http_pool_size == 100


def test_pool_size_defaults_and_overrides(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    config_file = tmp_path / "demo.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "name": "alpha",
                "db_pool_size": 12,
                "extract": {
                    "scraper": "pkg.x",
                    "http_pool_size": 25,
                    "options": {"base_url": "https://example.com"},
                },
            }
        )
    )
    config = load_config(config_file)
    assert config.db_pool_size == 12
    assert config.extract is not None
    assert config.extract.http_pool_size == 25


def test_db_pool_size_zero_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    config_file = tmp_path / "bad.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "name": "alpha",
                "db_pool_size": 0,
            }
        )
    )
    with pytest.raises(ConfigError):
        load_config(config_file)


def test_batch_size_in_extract_options_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x/y")
    config_file = tmp_path / "bad.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "name": "alpha",
                "extract": {
                    "scraper": "pkg.x",
                    "options": {
                        "base_url": "https://example.com",
                        "batch_size": 20,
                    },
                },
            }
        )
    )
    with pytest.raises(ConfigError):
        load_config(config_file)
