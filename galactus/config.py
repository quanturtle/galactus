from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field


class ExtractConfig(BaseModel):
    """Extract block of one source: which scraper strategy + its options."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    module: str
    scraper: str
    concurrency: int = Field(default=1, ge=1)
    options: dict[str, Any] = Field(default_factory=dict)


class TransformConfig(BaseModel):
    """Transform block of one source: which parser strategy + its options."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    module: str
    parser: str
    options: dict[str, Any] = Field(default_factory=dict)


class HttpConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    timeout_seconds: float = 30.0
    user_agent: str = "galactus/0.2"


class HttpOverride(BaseModel):
    """Per-source override of HttpConfig fields. Unset fields fall back to the domain default."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    timeout_seconds: float | None = None
    user_agent: str | None = None


class SourceConfig(BaseModel):
    """One source declares its own bronze/silver targets and its extract/transform blocks.

    Sources are explicit about their schema (bronze/silver tables and conflict
    keys) rather than inheriting them from a domain — this is the single place
    that owns the per-source persistence shape.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    bronze_table: str
    silver_table: str
    bronze_conflict_keys: tuple[str, ...] = ("source", "source_url")
    silver_conflict_keys: tuple[str, ...] = ("source", "source_url")
    http: HttpOverride | None = None
    extract: ExtractConfig | None = None
    transform: TransformConfig | None = None


class DatabaseConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    dsn: str
    min_pool_size: int = 1
    max_pool_size: int = 10


class BlobStoreConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    bucket: str
    region: str | None = None
    prefix: str = ""


class PipelineConfig(BaseModel):
    """Frozen, fully-typed runtime configuration. Loaded once at program startup."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    sources: list[SourceConfig]
    database: DatabaseConfig
    http: HttpConfig = Field(default_factory=HttpConfig)
    blob_store: BlobStoreConfig | None = None
    log_level: str = "INFO"


def resolve_http(domain: HttpConfig, override: HttpOverride | None) -> HttpConfig:
    """Deep-merge an HttpOverride onto the domain HttpConfig per-key.

    Unset (None) override fields fall through to the domain values. Returns a
    fresh frozen HttpConfig — neither input is mutated.
    """
    if override is None:
        return domain
    fields = domain.model_dump()
    for key, value in override.model_dump(exclude_none=True).items():
        fields[key] = value
    return HttpConfig.model_validate(fields)


def load_config(path: str | Path) -> PipelineConfig:
    """Read the single YAML config file and parse it into a frozen PipelineConfig.

    Called exactly once at program startup; nothing else in the codebase reads
    files or env vars to derive runtime configuration (rule 6).
    """
    config_path = Path(path)
    raw = yaml.safe_load(config_path.read_text())
    return PipelineConfig.model_validate(raw)
