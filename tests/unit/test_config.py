from pathlib import Path

import pytest
import yaml

from galactus.config import HttpConfig, HttpOverride, load_config, resolve_http


CONFIGS = Path(__file__).resolve().parents[2] / "configs"


def test_noticias_config_loads() -> None:
    config = load_config(CONFIGS / "noticias.yaml")
    assert config.domain == "noticias"
    names = [s.name for s in config.sources]
    assert "ultimahora" in names and "abc_color" in names


def test_supermercados_config_loads() -> None:
    config = load_config(CONFIGS / "supermercados.yaml")
    assert config.domain == "supermercados"
    names = [s.name for s in config.sources]
    assert "biggie" in names and "stock" in names


def test_default_concurrency_is_one() -> None:
    config = load_config(CONFIGS / "noticias.yaml")
    for source in config.sources:
        assert source.extract is not None
        assert source.extract.concurrency >= 1


def test_explicit_concurrency_parses(tmp_path: Path) -> None:
    domain = tmp_path / "demo.yaml"
    domain.write_text(
        yaml.safe_dump(
            {
                "domain": "demo",
                "database": {"dsn": "postgresql://x/y"},
            }
        )
    )
    sources_dir = tmp_path / "demo"
    sources_dir.mkdir()
    (sources_dir / "alpha.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "alpha",
                "extract": {"scraper": "x", "concurrency": 7, "options": {}},
            }
        )
    )
    config = load_config(domain)
    assert config.sources[0].extract is not None
    assert config.sources[0].extract.concurrency == 7


def test_missing_sources_dir_raises(tmp_path: Path) -> None:
    domain = tmp_path / "lonely.yaml"
    domain.write_text(
        yaml.safe_dump(
            {
                "domain": "lonely",
                "database": {"dsn": "postgresql://x/y"},
            }
        )
    )
    with pytest.raises(ValueError, match="sources directory not found"):
        load_config(domain)


def test_empty_sources_dir_raises(tmp_path: Path) -> None:
    domain = tmp_path / "empty.yaml"
    domain.write_text(
        yaml.safe_dump(
            {
                "domain": "empty",
                "database": {"dsn": "postgresql://x/y"},
            }
        )
    )
    (tmp_path / "empty").mkdir()
    with pytest.raises(ValueError, match="no source files"):
        load_config(domain)


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
    domain = tmp_path / "demo.yaml"
    domain.write_text(
        yaml.safe_dump(
            {
                "domain": "demo",
                "database": {"dsn": "postgresql://x/y"},
                "http": {"timeout_seconds": 30.0, "user_agent": "ua/1"},
            }
        )
    )
    sources_dir = tmp_path / "demo"
    sources_dir.mkdir()
    (sources_dir / "alpha.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "alpha",
                "http": {"timeout_seconds": 90.0},
                "extract": {"scraper": "x", "options": {}},
            }
        )
    )
    config = load_config(domain)
    assert config.sources[0].http is not None
    merged = resolve_http(config.http, config.sources[0].http)
    assert merged.timeout_seconds == 90.0
    assert merged.user_agent == "ua/1"


def test_concurrency_zero_rejected(tmp_path: Path) -> None:
    domain = tmp_path / "bad.yaml"
    domain.write_text(
        yaml.safe_dump(
            {
                "domain": "bad",
                "database": {"dsn": "postgresql://x/y"},
            }
        )
    )
    sources_dir = tmp_path / "bad"
    sources_dir.mkdir()
    (sources_dir / "src.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "src",
                "extract": {"scraper": "x", "concurrency": 0, "options": {}},
            }
        )
    )
    with pytest.raises(Exception):
        load_config(domain)
