import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from galactus.core.errors import ConfigError



class ExtractOptions(BaseModel):
    """Scraper-strategy options: URLs, patterns, pagination, and rate-limiting."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    base_url: str
    scrape_url_patterns: list[str] = []
    ignore_url_patterns: list[str] = []
    page_size: int = 0
    max_pages: int = 0
    batch_size: int = 0
    request_delay: float = 0.0


class ExtractConfig(BaseModel):
    """Extract block: which scraper strategy, HTTP knobs, and its options."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    scraper: str
    concurrency: int = Field(default=1, ge=1)
    timeout_seconds: float = 30.0
    user_agent: str = "galactus/0.2"
    options: ExtractOptions


class TransformConfig(BaseModel):
    """Transform block: which parser strategy and its options."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    parser: str
    options: dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    """One source, fully configured for one pipeline run."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    bronze_table: str
    silver_table: str
    dsn: str
    log_level: str = "INFO"
    extract: ExtractConfig | None = None
    transform: TransformConfig | None = None


def load_config(path: str | Path) -> PipelineConfig:
    """Read a per-source YAML file and return a frozen PipelineConfig.

    DSN is injected from the DATABASE_URL env var — it must not appear in the
    yaml file. Called exactly once at program startup (rule 6).
    """
    config_path = Path(path)
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise ConfigError("DATABASE_URL env var is required")
    try:
        raw = yaml.safe_load(config_path.read_text()) or {}
        raw["dsn"] = dsn
        return PipelineConfig.model_validate(raw)
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {config_path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"YAML parse error in {config_path}: {exc}") from exc
    except ValidationError as exc:
        raise ConfigError(f"invalid config: {exc}") from exc
