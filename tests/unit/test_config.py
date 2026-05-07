from pathlib import Path

from galactus.config import load_config


def test_noticias_config_loads() -> None:
    config = load_config(Path(__file__).resolve().parents[2] / "configs/noticias.yaml")
    assert config.domain == "noticias"
    assert config.schema_module == "galactus.domains.noticias.schema"
    names = [s.name for s in config.sources]
    assert "ultimahora" in names and "abc_color" in names


def test_supermercados_config_loads() -> None:
    config = load_config(Path(__file__).resolve().parents[2] / "configs/supermercados.yaml")
    assert config.domain == "supermercados"
    names = [s.name for s in config.sources]
    assert "biggie" in names and "stock" in names
