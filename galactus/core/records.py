from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel


@dataclass(frozen=True, slots=True)
class ApiSnapshot:
    """Bronze row destined for bronze.api_snapshots.

    Produced by API-style scrapers. The raw response body is stored as bytes;
    request_url and request_params are kept so requests can be replayed verbatim.
    """

    source: str
    source_url: str
    fetched_at: datetime
    request_url: str
    request_params: dict[str, Any]
    status_code: int
    response_headers: dict[str, str]
    body: bytes
    bronze_id: int | None = None
    kind: Literal["api_snapshot"] = "api_snapshot"


@dataclass(frozen=True, slots=True)
class HtmlSnapshot:
    """Bronze row destined for bronze.html_snapshots.

    Produced by BFS/HTML scrapers. Stores the raw HTML and minimal HTTP metadata.
    """

    source: str
    source_url: str
    fetched_at: datetime
    status_code: int
    content_type: str
    response_headers: dict[str, str]
    html: bytes
    bronze_id: int | None = None
    kind: Literal["html_snapshot"] = "html_snapshot"


# Discriminated union — every bronze row is one or the other.
RawRecord = ApiSnapshot | HtmlSnapshot


@dataclass(frozen=True, slots=True)
class ParsedRecord:
    """Silver-layer record: a typed domain entity plus its bronze provenance.

    `entity` is a pydantic model (Article, Product, ...) defined in the domain's
    schema module. Database.upsert is keyed by (source, source_url).
    """

    source: str
    source_url: str
    bronze_id: int
    parsed_at: datetime
    entity: BaseModel
    extras: dict[str, Any] = field(default_factory=dict)
