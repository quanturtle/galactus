from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field


class ExtractConfig(BaseModel):
    """Extract block of one source: which scraper strategy + its options."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    scraper: str
    concurrency: int = Field(default=1, ge=1)
    options: dict[str, Any] = Field(default_factory=dict)


class TransformConfig(BaseModel):
    """Transform block of one source: which parser strategy + its options."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    parser: str
    options: dict[str, Any] = Field(default_factory=dict)


class SourceConfig(BaseModel):
    """One source declares both its extract and transform blocks (either may be omitted)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    extract: ExtractConfig | None = None
    transform: TransformConfig | None = None


class DatabaseConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    dsn: str
    min_pool_size: int = 1
    max_pool_size: int = 10


class HttpConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    timeout_seconds: float = 30.0
    user_agent: str = "galactus/0.2"


class BlobStoreConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    bucket: str
    region: str | None = None
    prefix: str = ""


class PipelineConfig(BaseModel):
    """Frozen, fully-typed runtime configuration. Loaded once at program startup."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    domain: str
    sources: list[SourceConfig]
    database: DatabaseConfig
    http: HttpConfig = Field(default_factory=HttpConfig)
    blob_store: BlobStoreConfig | None = None
    log_level: str = "INFO"


def load_config(path: str | Path) -> PipelineConfig:
    """Read a domain YAML and per-source YAMLs and parse them into a frozen PipelineConfig.

    Layout: `configs/<domain>.yaml` holds domain-level settings; per-source
    files live in `configs/<domain>/*.yaml`. Called exactly once at program
    startup; nothing else in the codebase reads files or env vars to derive
    runtime configuration (rule 6).
    """
    domain_path = Path(path)
    raw = yaml.safe_load(domain_path.read_text())
    sources_dir = domain_path.parent / domain_path.stem
    if not sources_dir.is_dir():
        raise ValueError(f"sources directory not found: {sources_dir}")
    source_files = sorted(sources_dir.glob("*.yaml"))
    if not source_files:
        raise ValueError(f"no source files in {sources_dir}")
    raw["sources"] = [yaml.safe_load(f.read_text()) for f in source_files]
    return PipelineConfig.model_validate(raw)
