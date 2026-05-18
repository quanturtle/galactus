# galactus

<p align="center">
  <img src="img/logo.jpg" alt="galactus" width="320">
</p>

Async, staged web-scraping pipeline. One reusable package — `galactus` — drives per-source pipelines across multiple domains (currently **noticias** — Paraguayan news sites — and **supermercados** — supermarket chains). Every run flows through three stages — **extract → transform → load** — feeding a **bronze → silver → gold** medallion data model in Postgres.

```
internet ──▶ extract ──▶ bronze.{html,api}_snapshots ──▶ transform ──▶ silver.{articles,products} ──▶ load ──▶ gold.* (TBD)
              (scrape)         (raw captures)               (parse)         (normalized entities)       (aggregate)
```

## Quick start

```bash
# 1. Environment — set DATABASE_URL (a dev .env is checked in for local use)
#    DATABASE_URL=postgresql://galactus:galactus_secret@localhost:5432/galactus

# 2. Postgres 16 + Airflow (init, scheduler, webserver) + a one-shot `galactus-migrate` service
docker compose up -d

# 3. Install the package (Python >= 3.12)
uv sync

# 4. Apply DB migrations outside Docker (compose runs `alembic upgrade head` for you via galactus-migrate)
uv run alembic upgrade head

# 5. Run a source's full pipeline (extract -> transform -> load)
uv run galactus --config configs/lanacion.yaml

# ...or one stage at a time (this is what the Airflow DAGs do)
uv run galactus --config configs/superseis.yaml --stage extract
uv run galactus --config configs/superseis.yaml --stage transform
```

> **Note** — unlike the previous `galactus`, there is **no `galactus migrate` subcommand**. The CLI takes exactly two flags: `--config <path>` (required) and `--stage <name>` (optional; one of `extract`, `transform`, `load`). Schema work goes through `alembic` directly (see [Schema & migrations](#schema--migrations)).

## Project structure

```
galactus_v2/
├── galactus/                       # the package — domain-agnostic pipeline core
│   ├── cli.py                      # entrypoint: parse --config/--stage, validate plugins, build & run Pipeline
│   ├── config.py                   # Pydantic frozen config; load_config() reads YAML + injects DATABASE_URL
│   ├── core/
│   │   ├── pipeline.py             # Pipeline + PipelineStage(ABC) — the composition root
│   │   └── errors.py               # exception hierarchy: PipelineError -> Extract/Transform/Load/Infra/Config
│   ├── extract/
│   │   ├── base_scraper.py         # BaseScraper — async BFS crawler with a small public hook surface
│   │   ├── stage.py                # ExtractStage — adapts a Scraper into a PipelineStage
│   │   └── scrapers/{noticias,supermercados}/<source>.py   # per-source Scraper subclasses
│   ├── transform/
│   │   ├── base_parser.py          # BaseParser(ABC) — bronze->silver streaming lifecycle (decode -> build_item -> build_entity -> stamp)
│   │   ├── article_parser.py       # ArticleParser mixin — eight extract_* hooks + build_entity for silver.articles
│   │   ├── product_parser.py       # ProductParser mixin — eight extract_* hooks + build_entity for silver.products
│   │   ├── html_parser.py          # HtmlParser — ordered blocklist filter passes; run(text) -> BeautifulSoup
│   │   ├── stage.py                # TransformStage — adapts a Parser into a PipelineStage
│   │   └── parsers/{noticias,supermercados}/<source>.py    # per-source Parser(BaseParser, ArticleParser|ProductParser)
│   ├── load/stage.py               # LoadStage — stub for the future gold-layer aggregation
│   └── infra/
│       ├── db.py                   # Database — async SQLAlchemy engine; insert(), load_visited_requests(), stream_unparsed(); zstd compress/decompress
│       ├── http.py                 # HttpClient / HttpRequest / HttpResponse — httpx wrapper: pooling, retry
│       └── logging.py              # setup_logging()
├── sql/                            # ORM models (the schema source the migrations autogenerate from)
│   ├── base.py                     # Base(DeclarativeBase) with to_dict()
│   ├── a_bronze/                   # api_snapshots.py, html_snapshots.py, schema.py — bronze: two generic tables
│   ├── b_silver/                   # article.py, product.py, schema.py — silver: per-domain entities
│   └── c_gold/                     # schema.py only — gold layer is a stub
├── migrations/                     # Alembic (env.py: psycopg3 dialect, multi-schema, autogenerate)
├── configs/<source>.yaml           # one YAML per source
├── airflow/
│   ├── Dockerfile
│   └── dags/<source>_pipeline.py   # one DAG per source: extract >> transform BashOperators
├── docker-compose.yml              # Postgres 16 + airflow-init + galactus-migrate + scheduler + webserver
├── pyproject.toml                  # name=galactus, v0.2.0, script: galactus = galactus.cli:main
├── alembic.ini
└── tests/{unit,integration}/
```

## Architecture

```mermaid
flowchart TD
    cli["cli.main()"] --> lc["load_config()<br/>(reads YAML + DATABASE_URL once)"]
    cli --> vp["validate_plugins()<br/>(import Scraper / Parser, fail fast)"]
    cli --> bp["build_pipeline()"]
    bp --> P["Pipeline<br/>(ordered stages, dispatch by name)"]
    P --> ES["ExtractStage"]
    P --> TS["TransformStage"]
    P --> LS["LoadStage (stub)"]

    ES -->|owns for the run| HC["HttpClient"]
    ES -->|owns for the run| DB1["Database"]
    ES -->|imports by config path| SC["Scraper<br/>(BaseScraper subclass)"]
    SC -->|fetch| WEB[("websites / APIs")]
    SC -->|insert (skip-if-seen-today)| BRZ[("bronze schema")]

    TS -->|owns for the run| DB2["Database"]
    TS -->|imports by config path| PR["Parser<br/>(BaseParser + ArticleParser/ProductParser mixin)"]
    PR -->|stream_unparsed| BRZ
    PR -->|insert per bronze record| SLV[("silver schema")]

    LS -.future.-> GLD[("gold schema")]
```

The pipeline is small and explicit; each piece is built around a named design pattern.

### `core/pipeline.py` — `Pipeline` / `PipelineStage` · *Composition root + Strategy*
`Pipeline` owns an ordered `list[PipelineStage]` plus a `{name: stage}` index. `run(stage_name=None)` runs every stage in order; `run("extract")` runs just that one. Each stage is an interchangeable strategy hidden behind the abstract `async run()` — adding a fourth stage is "append it to `stages`". Construction-time invariants (non-empty, no duplicate names) are enforced up front.

### `config.py` — `PipelineConfig` (frozen Pydantic) · *Configuration object, read at the edges*
`load_config(path)` is the **single** read of the YAML file plus the `DATABASE_URL` env var, called once at startup; everything downstream gets the typed, frozen object. `PipelineConfig` composes a flat `ExtractConfig` (base URL, URL patterns, pagination, pacing, HTTP knobs) and `TransformConfig` (HTML blocklists), each keeping its stage's knobs together. `extra="forbid"` means a typo in a YAML key is a startup error, not silent drift.

### `cli.py` — `main()` / `validate_plugins()` · *Fail-fast composition + boundary error handling*
`validate_plugins()` imports the configured scraper and parser modules and checks they export `Scraper` / `Parser` *before* any I/O happens, so a bad config name fails immediately. `main()` is the one place that turns exceptions into exit codes: `ConfigError → 2`, `PipelineError → 1`, everything else propagates. Internal code never catches-and-logs.

### `core/errors.py` — exception hierarchy · *Layered exceptions, translated only at the boundary*
```
PipelineError
├── ExtractError      └── ScraperError      (one source failed to fetch)
├── TransformError    └── ParserError       (one source failed to parse)
├── LoadError
├── ConfigError                              (bad/missing config, unknown plugin)
└── InfraError ── HttpError, DatabaseError   (I/O adapter failure)
```
Infra adapters raise `HttpError` / `DatabaseError`; plugin code catches those and re-raises as `ScraperError` / `ParserError` with source + URL context; stages wrap anything that escapes as their `*Error`; the CLI is the only catcher.

### `extract/stage.py`, `transform/stage.py`, `load/stage.py` — `ExtractStage` / `TransformStage` / `LoadStage` · *Adapter*
Each stage adapts a domain object to the `PipelineStage` contract: it opens the infra context managers it needs (`HttpClient` + `Database` for extract, `Database` for transform), `importlib`-resolves the configured plugin module (`galactus.extract.scrapers.<dotted.path>`), instantiates it, awaits its `run()`, and re-raises failures as the stage's error type. `LoadStage` is a no-op stub today — the constructor matches the others so the wiring in `cli.py` stays uniform.

### `extract/base_scraper.py` — `BaseScraper` · *Template Method*
`run()` fixes the whole crawl lifecycle: pre-load the BFS `seen` set, seed a frontier deque from `seed_urls()`, spawn-and-drain up to `concurrency` in-flight `fetch()` tasks, and for each completed fetch run `process_response()` and fold its returned next URLs back into the frontier — then self-throttle by `request_delay`.

**Lifecycle (run()).**
- `seen_today()` pre-loads the BFS `seen` set so a same-day rerun re-fetches seeds (to discover new content) but skips any request already captured today; same-day dedup lives here rather than in the database.
- Spawn-and-drain is the only place that bounds fetch concurrency. `max_pages` is a **hard** cap on dispatched fetches, counted at spawn time.
- For each completed fetch, `process_response()` persists the snapshot if it passes `should_persist(request)` (record shape routed on the `snapshot_model` class var → `Database.insert(...)`), then returns `get_next_urls()` which is folded into the frontier through `should_enqueue()`.
- `should_enqueue` defaults: same-host, drop asset extensions, no `ignore_patterns` match, reject paths whose segments repeat (the symptom of bad relative-link resolution).
- Each completed task is wrapped in its own try/except, so one fetch / persist / next-url failure logs a warning and skips the URL instead of aborting the source. A `finally` drains in-flight tasks so a mid-run raise can't leak.

**Hook surface.** Small and orthogonal — `seed_urls`, `fetch`, `extract_links`, `build_url`, `get_next_urls`, `should_enqueue`, `should_persist`, `process_response`, plus the per-source transport tweaks `http_extras()` / `db_extras()`. All ship with working defaults keyed on `snapshot_model`:
- `HtmlSnapshot` ⇒ scrape every `<a href>` and store zstd-compressed HTML.
- `ApiSnapshot` ⇒ store the zstd-compressed raw body.

**`build_url(url, params=None)`.** Canonicalizes outgoing requests (lowercase scheme + host, strip `TRACKING_PARAMS` — utm/fbclid/gclid/…, drop the fragment) and returns an `HttpRequest`. Paginated APIs override it with a pagination signature (`build_url(page)`, `build_url(offset)`, …) called from their own `seed_urls()` / `get_next_urls()` overrides. There is no separate `BaseApiScraper`.

### `transform/base_parser.py` — `BaseParser` · *Template Method*
`run()` fixes the bronze→silver lifecycle as a streaming loop: open `Database`, then for each bronze row yielded by `stream_unparsed(...)` run `process_record` and `Database.insert()` the resulting silver rows **per record** before moving on.

**Lifecycle.**
- `Database.stream_unparsed(bronze_model, silver_model, source, chunk_size=batch_size)` yields bronze rows with no matching `(source, bronze_id)` in silver, in `(created_at, id)` order.
- `process_record`: `decode(record)` → `build_item(decoded)` → `build_entity(item)` per item → `stamp(entity, record)` with the bronze row's `id` and `created_at`.
- Insert is per record, not batched across records.
- A bronze row that fails decode/build raises `ParserError`, is logged, and is skipped — its silver does not commit, so the next run retries it through `stream_unparsed`.

**Required overrides.** Concrete parsers must set `silver_model` and mix in `ArticleParser` or `ProductParser` to contribute `build_entity` + the eight `extract_*` hooks. `bronze_model` defaults to `HtmlSnapshot`.

**Default behavior.**
- `build_item(decoded)` defaults to `[decoded]` (one entity per bronze record) — override only for listing-style payloads that pack many entities into one record.
- `decode()` defaults dispatch on `bronze_model`: `HtmlSnapshot` → `HtmlParser.run(self.db.decompress(record.html))` (a BeautifulSoup tree, blocklists already applied); `ApiSnapshot` → `json.loads(self.db.decompress(record.body))`.
- `make_html_parser()` is the per-parser hook for code-level blocklist defaults.

**Idempotency.** No dedup here — one silver row per `(entity, bronze sighting)`; collapsing across sightings is the gold layer's job. Re-runs are safe: a bronze row counts as parsed once *any* silver row carries its `(source, bronze_id)`, so `stream_unparsed()` skips it next time.

### `transform/article_parser.py`, `transform/product_parser.py` — `ArticleParser` / `ProductParser` · *Mixin (role contribution)*
Field-extraction mixins composed alongside `BaseParser` (`class Parser(BaseParser, ArticleParser): ...`). Each mixin owns `build_entity(item) -> Article | Product` and declares eight abstract `extract_*` hooks in the same order the columns appear in the silver model — so a parser file reads top-to-bottom against the silver schema. Every silver field is optional, so the hooks return whatever they can find (or an empty value); `build_entity` does not filter.

**`ArticleParser` hooks** (order = `silver.articles` columns): `source_url`, `title`, `body`, `authors`, `published_at`, `section`, `tags`, `image_urls`.

**`ProductParser` hooks** (order = `silver.products` columns): `source_url`, `sku`, `name`, `brand`, `price`, `currency`, `unit`, `image_urls`. Also ships `parse_unit_from_name(name)` — an ordered regex list (`kg`, `l`, `ml`, `g`, `cc`, in match-priority order) that recovers the inline unit info embedded in ~80% of supermercado product names; concrete parsers delegate `extract_unit` to it when no structured field is available.

**About `item`.** Whatever `BaseParser.build_item` yielded — typically a dict bundling the per-entity slice with any bronze-derived context the hooks need (e.g. `source_url` for HTML single-page parsers whose payload doesn't carry the URL).

### `extract/scrapers/<domain>/<source>.py`, `transform/parsers/<domain>/<source>.py` · *Strategy / plugin*
Each module exports a single `Scraper` (or `Parser`) class subclassing the template-method base; parsers also mix in `ArticleParser` or `ProductParser`.

**Selection.** The plugin is selected by **dotted path in the YAML** — `extract.scraper: noticias.lanacion` resolves to `galactus.extract.scrapers.noticias.lanacion.Scraper`. There is no registry; the CLI just imports the path and checks the class is there.

**HTML scrapers.** Typically one-liners — set `snapshot_model = HtmlSnapshot` and the defaults handle BFS, link extraction, and zstd storage.

**API scrapers.** Set `snapshot_model = ApiSnapshot`, override `seed_urls()` / `get_next_urls()`, and define a paginating `build_url(...)`. Examples: `scrapers/noticias/lanacion.py` walks an Arc Publishing feed by offset; `scrapers/noticias/abc_color.py` walks each section, paginated.

**Parsers.** Implement the eight `extract_*` hooks against the bronze payload. Examples: `parsers/supermercados/superseis.py` reads JSON-LD Product on every page; `parsers/noticias/lanacion.py` builds many Article items per bronze record via `build_item` over an Arc PF feed.

### `infra/http.py` — `HttpClient` / `HttpRequest` / `HttpResponse` · *Adapter*
`HttpClient` wraps `httpx.AsyncClient` (connection-pool `Limits`, `follow_redirects=True`); fetch concurrency is `BaseScraper.run`'s job, not this client's. `get(request)` is single-attempt: it returns any response with status `< 500` and raises `HttpError` on `5xx` or transient transport errors (connect errors, timeouts, mid-stream disconnects). `BaseScraper.run` turns that `HttpError` into a per-URL skip, and same-day reruns re-fetch via `seen_today` so transient failures self-heal across runs. `HttpRequest` is a hashable value object (`url`, `headers`, `params`) — `BaseScraper` uses `hash(request)` as the BFS `seen` key. `HttpResponse` exposes only `status_code` / `headers` / `content` / `text` / `json()` / `request` — scrapers never touch `httpx` directly.

### `infra/db.py` — `Database` · *Repository / data-access gateway*
**One configurable class, not one repo per layer.** It owns a single `AsyncEngine` + `async_sessionmaker`, registers the psycopg3 dialect for bare `postgresql://` URLs, and verifies connectivity in `open()` (used as an `async with`). Three async operations plus the two codec helpers:
- `insert(records, model)` — bulk insert via SQLAlchemy `insert`; columns that are `None` on **every** row are dropped from the row dicts so the database applies its own defaults (surrogate ids, server-filled `created_at`). No `ON CONFLICT` clause — same-day re-fetch dedup lives in `BaseScraper.seen_today`.
- `load_visited_requests(model, source)` — `(source_url, request_params)` tuples for `source` captured (2xx only) since UTC midnight. `request_params` is `{}` for models without that column (`html_snapshots`); for `api_snapshots` it carries the per-page query so paginated requests hash distinctly under the same base URL. Powers `BaseScraper.seen_today()`.
- `stream_unparsed(bronze_model, silver_model, source, chunk_size=100)` — async generator: bronze rows for `source` with no matching `(source, bronze_id)` in silver, ordered by `(created_at, id)`, server-streamed via `yield_per` so memory stays bounded.
- `compress(text) -> bytes` / `decompress(blob) -> str` — zstd level 6 (per-instance compressor / decompressor) for `BYTEA` columns. Scrapers `compress` on the way into bronze; parsers `decompress` on the way out.

All queries are SQLAlchemy constructs (`select`, `insert`, `.exists()`) — no interpolated SQL.

### `transform/html_parser.py` — `HtmlParser` · *Pipeline of filters*
`run(text)` builds a BeautifulSoup (`lxml`) tree and runs ordered passes over it in declaration order: strip HTML comments → `decompose` each tag in `blocklist_tags` (tag + subtree) → delete each `blocklist_attributes` entry from every remaining tag. `blocklist_tags` is the union of `BASELINE_BLOCKLIST_TAGS = ("script", "style", "noscript")` and any per-source additions; `<script type="application/ld+json">` is **always preserved** so source parsers can read structured data from it (e.g. `superseis` reads its Product JSON-LD here).

### `sql/` — `Base` + per-layer `schema.py` · *ORM declarative base + DDL hook*
`Base(DeclarativeBase)` adds `to_dict()` (used by `Database.insert` to materialize per-row dicts). Each `sql/<layer>/schema.py` registers a `CREATE SCHEMA IF NOT EXISTS <layer>` listener on `Base.metadata`'s `before_create` event. Importing `sql` (done by `migrations/env.py` and by the model imports in scrapers/parsers) registers every table and hook. Tables:
- **bronze** — two generic capture tables, `bronze.api_snapshots` and `bronze.html_snapshots`. Each has an `id` PK and indexes on `source`, `source_url`, and `created_at`; there is no natural-key uniqueness — re-fetch dedup lives in the scraper (`seen_today()`), not the database.
- **silver** — per-domain normalized entities, `silver.articles` and `silver.products`, each carrying provenance (`bronze_id` → bronze `id`, plus the bronze snapshot's `created_at` stamped at parse time).
- **gold** — schema only; aggregated/deduplicated entities are TBD.

### `migrations/env.py` — Alembic environment · *Migration manager*
Registers the psycopg3 dialect (so `DATABASE_URL` stays a plain `postgresql://` — it is not rewritten), `import sql` to populate `target_metadata`, `ensure_schemas()` to `CREATE SCHEMA IF NOT EXISTS` for bronze/silver/gold before any migration runs, `include_name()` to restrict autogenerate to galactus-owned schemas (Airflow shares the database and owns `public`), and a `galactus_alembic_version` table in `public`.

### Patterns at a glance

| Pattern | Where | Role |
|---|---|---|
| Composition root | `core/pipeline.py` `Pipeline` | owns and sequences the stages |
| Strategy | `PipelineStage` impls; concrete scrapers/parsers selected by config path | swap behavior without touching the core |
| Template Method | `extract/base_scraper.py` `BaseScraper.run()` (spawn-and-drain BFS), `transform/base_parser.py` `BaseParser.run()` (streaming bronze→silver, per-record insert) | fixed lifecycle, a few narrow override hooks |
| Mixin (role contribution) | `transform/article_parser.py` `ArticleParser`, `transform/product_parser.py` `ProductParser` | contribute `build_entity` + eight `extract_*` hooks per silver entity, composed with `BaseParser` via MRO |
| Adapter | `extract/stage.py` / `transform/stage.py` / `load/stage.py`; `infra/http.py` `HttpClient` / `HttpRequest` / `HttpResponse` | bridge domain objects & httpx to the pipeline / scraper contracts |
| Repository / data-access | `infra/db.py` `Database` | one configurable persistence gateway (`insert`, `load_visited_requests`, `stream_unparsed`; zstd `compress`/`decompress`) |
| Pipeline of filters | `transform/html_parser.py` `HtmlParser` | ordered, composable HTML-cleaning passes |
| Configuration object (edges-only) | `config.py` `PipelineConfig` + `load_config()` | one typed, frozen read at startup |
| Layered exception hierarchy | `core/errors.py` | categorize failures by layer; translate to exit codes only at the CLI boundary |
| ORM declarative base + DDL hook | `sql/base.py`, `sql/*/schema.py` | shared model base; auto-create the layer schemas |
| Migration manager | `migrations/env.py` | versioned, multi-schema, psycopg3, autogenerated migrations |
| Plugin discovery / fail-fast | `cli.py` `validate_plugins()` | import + validate the configured source modules before running |

## Data pipeline

```mermaid
flowchart LR
    SRC[("website / API")] -->|"Scraper.run() — BFS / paginated fetch"| EX[extract]
    EX -->|"process_response, insert (skip-if-seen-today)"| BR{{"bronze.html_snapshots<br/>bronze.api_snapshots<br/>(raw captures, id PK + zstd-compressed body)"}}
    BR -->|"stream_unparsed → decode → build_item → build_entity"| TR[transform]
    TR -->|"insert per record, stamped with (source, bronze_id, created_at)"| SV{{"silver.articles<br/>silver.products<br/>(normalized entities + provenance)"}}
    SV -.->|"aggregate / dedup across sightings — TBD"| LD[load]
    LD -.-> GD{{"gold.* (TBD)"}}
```

Each source follows the **bronze/silver** medallion shape: capture raw bytes first, parse them into structured rows later.

| Domain & source kind | bronze model | silver model | extract behavior |
|---|---|---|---|
| **noticias** — API sources (e.g. `lanacion`, `abc_color`) | `ApiSnapshot` | `Article` | paginated JSON feeds; pagination via `seed_urls()` / `get_next_urls()` overrides |
| **noticias** — HTML sources (e.g. `ultimahora`) | `HtmlSnapshot` | `Article` | same-domain BFS, zstd-compressed HTML body |
| **supermercados** — API sources (e.g. `biggie`, `grutter`) | `ApiSnapshot` | `Product` | paginated JSON product catalogs |
| **supermercados** — HTML sources (e.g. `superseis`) | `HtmlSnapshot` | `Product` | same-domain BFS over `/product/`-style URLs |

Design decisions worth knowing:
- **One silver row per (entity, bronze sighting).** Silver does no deduplication; collapsing repeated sightings of the same article/product is reserved for the gold layer (not yet built).
- **Provenance is `(source, bronze_id)`** on every silver row, plus the bronze snapshot's `created_at`.
- **Same-day re-runs are idempotent.** `seen_today()` pre-loads the BFS `seen` set from bronze rows captured (2xx) since UTC midnight, so re-scraping today re-fetches the seeds (to discover new content) but skips any request already in bronze; re-transforming skips bronze rows already referenced by silver. Re-runs on a later day re-fetch — each calendar day produces its own snapshot column in bronze.
- **HTML diff versioning is deferred.** `html_snapshots.is_diff` exists but is always `False` today — every fetch stores the full body.
- **Scheduling and run identity live outside the pipeline.** The CLI takes no `--run-id`; Airflow's metadata DB owns the run ledger.

## Scrapers & parsers

```mermaid
classDiagram
    class BaseScraper {
        <<Template Method>>
        +ClassVar snapshot_model
        +seed_urls() list~HttpRequest~
        +fetch(request) HttpResponse
        +extract_links(response) list~str~
        +build_url(url, params=None) HttpRequest
        +get_next_urls(response) list~HttpRequest~
        +should_enqueue(request) bool
        +should_persist(request) bool
        +process_response(response) list~HttpRequest~
        +seen_today() set~int~
        +run()
    }
    class BaseParser {
        <<Template Method>>
        +ClassVar bronze_model
        +ClassVar silver_model
        +decode(record) Any
        +build_item(decoded) list
        +stamp(entity, record)
        +process_record(record) list~Base~
        +run()
    }
    class ArticleParser {
        <<Mixin>>
        +extract_source_url / title / body / authors
        +extract_published_at / section / tags / image_urls
        +build_entity(item) Article
    }
    class ProductParser {
        <<Mixin>>
        +extract_source_url / sku / name / brand
        +extract_price / currency / unit / image_urls
        +parse_unit_from_name(name) str?
        +build_entity(item) Product
    }
    BaseScraper <|-- LanacionScraper : ApiSnapshot, paginates Arc feed
    BaseScraper <|-- AbcColorScraper : ApiSnapshot, paginates sections
    BaseScraper <|-- UltimahoraScraper : HtmlSnapshot, BFS
    BaseScraper <|-- SuperseisScraper : HtmlSnapshot, BFS over /product/
    BaseParser <|-- LanacionParser
    ArticleParser <|.. LanacionParser
    BaseParser <|-- SuperseisParser
    ProductParser <|.. SuperseisParser
```

A minimal HTML scraper is just the class var:

```python
from galactus.extract.base_scraper import BaseScraper
from sql.a_bronze.html_snapshots import HtmlSnapshot


class Scraper(BaseScraper):
    """Scraper for ultimahora — same-domain BFS into bronze.html_snapshots."""

    snapshot_model = HtmlSnapshot
```

An API scraper drives pagination through the hooks. The `url=` / `params=` keyword path on `build_url` is the one `seen_today()` uses to re-hash captured requests, so paginating subclasses keep it on the signature:

```python
import json
from typing import Any

from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for example — offset-paginated feed into bronze.api_snapshots."""

    snapshot_model = ApiSnapshot
    LIMIT = 100

    def build_url(
        self,
        offset: int | None = None,
        url: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> HttpRequest:
        query = json.dumps({"limit": str(self.LIMIT), "offset": str(offset)})
        return HttpRequest(
            url=url if url is not None else self.config.base_url,
            headers=dict(self.config.headers),
            params=params if params is not None else {"query": query},
        )

    def seed_urls(self) -> list[HttpRequest]:
        return [self.build_url(0)]

    def get_next_urls(self, response: HttpResponse) -> list[HttpRequest]:
        elements = response.json().get("content_elements", [])
        if len(elements) < self.LIMIT:
            return []
        blob = json.loads(response.request.params["query"])
        return [self.build_url(int(blob["offset"]) + self.LIMIT)]
```

A parser composes `BaseParser` with the right mixin and implements the eight `extract_*` hooks:

```python
from decimal import Decimal

from galactus.transform.base_parser import BaseParser
from galactus.transform.product_parser import ProductParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product


class Parser(BaseParser, ProductParser):
    """Parses HtmlSnapshots from <source>.com.py into Product entities."""

    bronze_model = HtmlSnapshot
    silver_model = Product

    # decode() default returns a BeautifulSoup tree; override only when the
    # parser wants to bundle extra bronze context (e.g. record.source_url)
    # into the `item` passed to extract_* hooks. See parsers/supermercados/superseis.py.

    def extract_source_url(self, item) -> str: ...
    def extract_sku(self, item) -> str | None: ...
    def extract_name(self, item) -> str: ...
    def extract_brand(self, item) -> str | None: ...
    def extract_price(self, item) -> Decimal | None: ...
    def extract_currency(self, item) -> str: ...
    def extract_unit(self, item) -> str | None:
        return self.parse_unit_from_name(self.extract_name(item))
    def extract_image_urls(self, item) -> list[str]: ...
```

## Schema & migrations

**Alembic is the single source of truth, and migrations are autogenerated — never hand-written.** The workflow is: edit the SQLAlchemy models under `sql/`, then

```bash
uv run alembic revision --autogenerate -m "describe the change"   # generate the migration from model changes
uv run alembic upgrade head                                       # apply it
uv run alembic current                                            # show the applied revision
uv run alembic history                                            # show the revision graph
uv run alembic downgrade -1                                        # roll back one step
```

`migrations/env.py` registers the psycopg3 dialect (DATABASE_URL stays `postgresql://`, no rewriting), creates the `bronze` / `silver` / `gold` schemas before migrating, restricts autogenerate to those schemas (Airflow shares the DB and owns `public`), and tracks state in a `galactus_alembic_version` table in `public`. Under Docker, the one-shot `galactus-migrate` service runs `alembic upgrade head` before the scheduler/webserver start.

## Adding a new source

A *source* is one website or API within a domain (`noticias` or `supermercados`).

### 1. YAML config — `configs/<source>.yaml`

```yaml
name: <source>
log_level: INFO
extract:
  scraper: supermercados.<source>            # dotted path under galactus.extract.scrapers
  base_url: https://www.<source>.com.py
  allowed_domains:
  - www.<source>.com.py
  scrape_patterns:                           # empty list = persist every fetched URL
  - /product/[a-z0-9-]+
  ignore_patterns:
  - /login
  - /cart
  max_pages: -1                              # hard cap on dispatched fetches; -1 = unbounded
  concurrency: 5                             # in-flight fetch tasks
  timeout_seconds: 30.0
transform:
  parser: supermercados.<source>             # dotted path under galactus.transform.parsers
  batch_size: 100                            # stream_unparsed chunk size; default 100
  blocklist_tags: []                         # populate to strip tag+subtree before parsing
  blocklist_attributes: []                   # populate to strip attributes from remaining tags
```

Paginated API sources don't take their page size from the config — they hard-code it on the scraper class (e.g. `LIMIT = 100`, `FEED_SIZE = 100`) since each API encodes it under its own JSON-blob query parameter.

### 2. Scraper — `galactus/extract/scrapers/<domain>/<source>.py`

Export a class **named `Scraper`** (the stage imports that exact name). For an HTML source, set `snapshot_model = HtmlSnapshot` and you're done. For an API source, also set `snapshot_model = ApiSnapshot` and override `seed_urls()` / `get_next_urls()` (and typically define your own paginating `build_url(...)` — keep the `url=` / `params=` keyword path on the signature so `seen_today()` can re-hash captured requests). See `scrapers/noticias/lanacion.py` and `scrapers/noticias/abc_color.py`.

### 3. Parser — `galactus/transform/parsers/<domain>/<source>.py`

Export a class **named `Parser`** that subclasses **both** `BaseParser` and one of `ArticleParser` / `ProductParser`. Set `silver_model` (and `bronze_model` if not `HtmlSnapshot`); implement the eight `extract_*` hooks from the mixin. Override `build_item(decoded)` when one bronze record carries many entities (listing-style payloads), `decode()` to bundle bronze-derived context into the per-entity `item`, and `make_html_parser()` to bake in code-level blocklist defaults.

### 4. Airflow DAG — `airflow/dags/<source>_pipeline.py`

Copy an existing DAG and change `SOURCE` / `SOURCE_TYPE`. Each is two `BashOperator`s, `extract >> transform`, shelling out to `galactus --config configs/<source>.yaml --stage <stage>`.

## Orchestration (Airflow)

`docker-compose.yml` runs the whole stack: `db` (Postgres 16, healthchecked), `airflow-init` (one-shot: `airflow db migrate` + create the `admin`/`admin` user), `galactus-migrate` (one-shot: invokes `/home/airflow/galactus/.venv/bin/alembic upgrade head` directly), `airflow-scheduler`, and `airflow-webserver` on `http://localhost:8080`. The galactus source, configs, and migrations are **bind-mounted** into the Airflow containers at `/home/airflow/galactus` (the DAGs `cd` there before running the CLI), and `airflow/dags/` is mounted too — so editing a DAG is picked up on the next scheduler scan, no rebuild.

Parallelism is capped on purpose: `AIRFLOW__CORE__PARALLELISM=3` (at most three task instances run concurrently across the whole scheduler) and `AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1` (one active run per source DAG at a time), so concurrent scrapers don't overwhelm the local Postgres or trip per-site rate limits.

```bash
docker compose up -d
open http://localhost:8080            # login: admin / admin
```

There is **one DAG per source** — `<source>_pipeline`, tagged `["pipeline", <domain>, <source>]` — with tasks `extract >> transform`:

```python
extract   = BashOperator(task_id="extract",   cwd=PROJECT_DIR,
                          bash_command=f"galactus --config configs/{SOURCE}.yaml --stage extract")
transform = BashOperator(task_id="transform", cwd=PROJECT_DIR,
                          bash_command=f"galactus --config configs/{SOURCE}.yaml --stage transform")
extract >> transform
```

Scheduling (and the run ledger) is Airflow's responsibility — the pipeline itself is stateless about runs.

## Development

```bash
uv sync --extra dev
uv run pytest                 # tests/unit/ (always) + tests/integration/ (needs a Postgres at DATABASE_URL)
uv run ruff check .
uv run ruff format .
```

`tests/unit/` covers the `Pipeline` composition, config loading/validation, the error hierarchy, the BFS / pagination shape of `BaseScraper`, the `BaseParser` lifecycle, per-parser field extraction for every source, and the import graph (it compiles, and `core/` imports nothing from the outer layers — the dependency direction stays one-way). `tests/integration/test_db.py` exercises `Database.insert()` and `Database.stream_unparsed()` against a real database via scratch-schema models.
