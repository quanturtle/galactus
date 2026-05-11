import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from galactus.core.errors import ConfigError


class ExtractOptions(BaseModel):
    """Scraper-strategy options: URLs, patterns, pagination, and per-task pacing."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    base_url: str
    allowed_hosts: list[str] = []
    scrape_url_patterns: list[str] = []
    ignore_url_patterns: list[str] = []
    page_size: int = 0
    max_pages: int = 0
    request_delay: float = 0.0


class ExtractConfig(BaseModel):
    """Extract block: which scraper strategy, HTTP knobs, fetch concurrency, and its options."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    scraper: str
    timeout_seconds: float = 30.0
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    retries: int = 3
    retry_delay: float = 2.0
    http_pool_size: int = Field(default=100, ge=1)
    concurrency: int = Field(default=1, ge=1)
    options: ExtractOptions


class TransformOptions(BaseModel):
    """Parser-strategy options: HTML cleaning rules."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    blocklist_tags: list[str] = []
    blocklist_attributes: list[str] = []


class TransformConfig(BaseModel):
    """Transform block: which parser strategy and its options."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    parser: str
    options: TransformOptions = Field(default_factory=TransformOptions)


class PipelineConfig(BaseModel):
    """One source, fully configured for one pipeline run."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    database_url: str
    log_level: str = "INFO"
    db_pool_size: int = Field(default=5, ge=1)
    extract: ExtractConfig | None = None
    transform: TransformConfig | None = None


def load_config(path: str | Path) -> PipelineConfig:
    """Read a per-source YAML file and return a frozen PipelineConfig.

    database_url is injected from the DATABASE_URL env var — it must not
    appear in the yaml file. Called exactly once at program startup (rule 6).
    """
    config_path = Path(path)
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ConfigError("DATABASE_URL env var is required")
    try:
        body = yaml.safe_load(config_path.read_text()) or {}
        body["database_url"] = database_url
        return PipelineConfig.model_validate(body)
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {config_path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"YAML parse error in {config_path}: {exc}") from exc
    except ValidationError as exc:
        raise ConfigError(f"invalid config: {exc}") from exc
