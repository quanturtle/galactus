import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from galactus.core.errors import ConfigError

# parent-to-child config propagation. INHERITED_FIELDS names the values
# trickled into each child sub-dict listed in INHERITED_CHILDREN; "source"
# is renamed from the parent's "name", the rest carry their name across.
INHERITED_FIELDS: tuple[str, ...] = ("source", "database_url", "db_pool_size")
INHERITED_CHILDREN: tuple[str, ...] = ("extract", "transform")


class ExtractConfig(BaseModel):
    """Extract block: scraper plugin, transport knobs, BFS settings, persistence target."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    source: str
    database_url: str
    db_pool_size: int = Field(default=5, ge=1)
    scraper: str
    timeout_seconds: float = 30.0
    headers: dict[str, str] = Field(
        default_factory=lambda: {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
    )
    params: dict[str, str] = {}
    retries: int = 3
    retry_delay: float = 2.0
    concurrency: int = Field(default=5, ge=1)
    base_url: str
    allowed_domains: frozenset[str] = frozenset()
    scrape_patterns: list[re.Pattern[str]] = Field(default_factory=list)
    ignore_patterns: list[re.Pattern[str]] = Field(default_factory=list)
    page_size: int = 0
    max_pages: int = -1
    request_delay: float = 0.0

    @model_validator(mode="before")
    @classmethod
    def compile_patterns(cls, body: Any) -> Any:
        """Compile pattern strings from yaml into re.Pattern objects."""
        if not isinstance(body, dict):
            return body
        for key in ("scrape_patterns", "ignore_patterns"):
            raw = body.get(key)
            if isinstance(raw, list):
                body[key] = [re.compile(p) if isinstance(p, str) else p for p in raw]
        return body


class TransformConfig(BaseModel):
    """Transform block: parser plugin and HTML cleaning rules."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str
    database_url: str
    db_pool_size: int = Field(default=5, ge=1)
    parser: str
    blocklist_tags: list[str] = []
    blocklist_attributes: list[str] = []


class PipelineConfig(BaseModel):
    """One source, fully configured for one pipeline run."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    database_url: str
    log_level: str = "INFO"
    db_pool_size: int = Field(default=5, ge=1)
    extract: ExtractConfig | None = None
    transform: TransformConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def trickle_inherited_fields(cls, body: Any) -> Any:
        """Push parent fields onto each child sub-dict before child validation.

        Maps the parent's `name` to the child's `source` (the only rename), then
        copies the rest by identity. A child key already set in yaml wins.
        """
        if not isinstance(body, dict):
            return body
        for child_key in INHERITED_CHILDREN:
            child = body.get(child_key)
            if not isinstance(child, dict):
                continue
            for parent_name in INHERITED_FIELDS:
                src_name = "name" if parent_name == "source" else parent_name
                if parent_name not in child and src_name in body:
                    child[parent_name] = body[src_name]
        return body


def load_config(path: str | Path) -> PipelineConfig:
    """Read a per-source YAML file and return a frozen PipelineConfig.

    database_url is injected from the DATABASE_URL env var — it must not
    appear in the yaml file. Called exactly once at program startup.
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
