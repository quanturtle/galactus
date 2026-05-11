# galactus

Async, staged web-scraping pipeline. One reusable package — `galactus` — drives **14 per-source pipelines** split across two domains: **noticias** (8 Paraguayan news sites) and **supermercados** (6 supermarket chains). Every run flows through three stages — **extract → transform → load** — feeding a **bronze → silver → gold** medallion data model in Postgres.

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
│   │   ├── base_scraper.py         # BaseScraper — async BFS crawler, three overridable hooks
│   │   ├── stage.py                # ExtractStage — adapts a Scraper into a PipelineStage
│   │   └── scrapers/{noticias,supermercados}/<source>.py   # per-source Scraper subclasses
│   ├── transform/
│   │   ├── base_parser.py          # BaseParser(ABC) — bronze->silver lifecycle, build_entities() hook
│   │   ├── html_parser.py          # HtmlParser (ordered blocklist filters) + compress/decompress/content-hash
│   │   ├── stage.py                # TransformStage — adapts a Parser into a PipelineStage
│   │   └── parsers/{noticias,supermercados}/<source>.py    # per-source Parser subclasses
│   ├── load/stage.py               # LoadStage — stub for the future gold-layer aggregation
│   └── infra/
│       ├── db.py                   # Database — async SQLAlchemy engine; insert() (ON CONFLICT DO NOTHING), load_unparsed()
│       ├── http.py                 # HttpClient / HttpResponse — httpx wrapper: pooling, concurrency, retry
│       └── logging.py              # setup_logging()
├── sql/                            # ORM models (the schema source the migrations autogenerate from)
│   ├── base.py                     # Base(DeclarativeBase) + to_dict()
│   ├── a_bronze/                   # api_snapshots.py, html_snapshots.py, schema.py — bronze: two generic tables
│   ├── b_silver/                   # article.py, product.py, schema.py — silver: per-domain entities
│   └── c_gold/                     # schema.py only — gold layer is a stub
├── migrations/                     # Alembic (env.py: psycopg3 dialect, multi-schema, autogenerate)
├── configs/<source>.yaml           # one YAML per source (14 of them)
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
    SC -->|insert ON CONFLICT DO NOTHING| BRZ[("bronze schema")]

    TS -->|owns for the run| DB2["Database"]
    TS -->|imports by config path| PR["Parser<br/>(BaseParser subclass)"]
    PR -->|load_unparsed| BRZ
    PR -->|insert| SLV[("silver schema")]

    LS -.future.-> GLD[("gold schema")]
```

The pipeline is small and explicit; each piece is built around a named design pattern.

### `core/pipeline.py` — `Pipeline` / `PipelineStage` · *Composition root + Strategy*
`Pipeline` owns an ordered `list[PipelineStage]` plus a `{name: stage}` index. `run(stage_name=None)` runs every stage in order; `run("extract")` runs just that one. Each stage is an interchangeable strategy hidden behind the abstract `async run()` — adding a fourth stage is "append it to `stages`". Construction-time invariants (non-empty, no duplicate names) are enforced up front.

### `config.py` — `PipelineConfig` (frozen Pydantic) · *Configuration object, read at the edges*
`load_config(path)` is the **single** read of the YAML file plus the `DATABASE_URL` env var, called once at startup; everything downstream gets the typed, frozen object. Nested models — `ExtractConfig` / `ExtractOptions` (base URL, URL patterns, pagination, pacing, HTTP knobs) and `TransformConfig` / `TransformOptions` (HTML blocklists) — keep each stage's knobs together. `extra="forbid"` means a typo in a YAML key is a startup error, not silent drift.

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
`run()` fixes the whole crawl lifecycle: seed a frontier deque from `seed_urls()`, spawn-and-drain up to `config.concurrency` in-flight fetch tasks, fetch each URL, persist it if it passes the `scrape_url_patterns` gate (`build_snapshot()` → `Database.insert(... ON CONFLICT DO NOTHING)`), canonicalize the page's outbound links (drop `mailto:`/`tel:`/`javascript:`/fragments/asset extensions, restrict to allowed hosts, sort query params, strip fragments) and enqueue the ones that pass the same-host + scrape/ignore-pattern gate, then self-throttle by `request_delay`. `max_pages` is a **soft** cap (in-flight tasks may overshoot by up to `concurrency-1`). Concrete scrapers override at most three hooks — `seed_urls()`, `next_urls()`, `build_snapshot()` — and **must** set the `bronze_model` class var; all three hooks ship with working defaults keyed on `bronze_model` (`HtmlSnapshot` ⇒ scrape every `<a href>` and store cleaned HTML; `ApiSnapshot` ⇒ store the raw body). There is no separate `BaseApiScraper` — paginated APIs just override `seed_urls()` / `next_urls()` to walk pages.

### `transform/base_parser.py` — `BaseParser` · *Template Method*
`run()` fixes the bronze→silver lifecycle: `load_records()` (all bronze rows for the source that no silver row references yet) → `parse_records()` (`decode()` each, then `build_entities()`, then stamp every entity with the bronze row's `bronze_id` and `created_at` as provenance) → `Database.insert()` of all silver rows in one transaction. Concrete parsers **must** set `bronze_model` / `silver_model` and implement `build_entities()`; `decode()` (BeautifulSoup for `HtmlSnapshot`, `json.loads` for `ApiSnapshot`) and `_make_html_parser()` ship with defaults. No dedup here — one silver row per `(entity, bronze sighting)`; collapsing across sightings is the gold layer's job. Re-runs are safe: a bronze row counts as parsed once *any* silver row carries its `(source, bronze_id)`, so `load_unparsed()` skips it next time.

### `extract/scrapers/<domain>/<source>.py`, `transform/parsers/<domain>/<source>.py` · *Strategy / plugin*
Each module exports a single `Scraper` (or `Parser`) class subclassing the template-method base. The plugin is selected by **dotted path in the YAML** — `extract.scraper: noticias.lanacion` resolves to `galactus.extract.scrapers.noticias.lanacion.Scraper`. There is no registry; the CLI just imports the path and checks the class is there. Most HTML sources are one-liners (`bronze_model = HtmlSnapshot`); API sources override `seed_urls()` / `next_urls()` (e.g. `noticias/lanacion.py` walks an Arc Publishing feed by offset; `noticias/abc_color.py` walks twelve sections, each paginated). Parser `build_entities()` bodies are currently stubs (`raise NotImplementedError`) — the parsing logic is in progress.

### `infra/http.py` — `HttpClient` / `HttpResponse` · *Adapter + Retry*
`HttpClient` wraps `httpx.AsyncClient` (connection-pool `Limits`, an `asyncio.Semaphore` capping concurrency, `follow_redirects=True`). `get()` returns any response with status `< 500`, retries `5xx` and transient transport errors (connect errors, timeouts, mid-stream disconnects) up to `retries` times with `retry_delay` backoff, and raises `HttpError` once exhausted. `HttpResponse` exposes only `status_code` / `headers` / `content` / `text` / `json()` — scrapers never touch `httpx` directly.

### `infra/db.py` — `Database` · *Repository / data-access gateway*
**One configurable class, not one repo per layer.** It owns a single `AsyncEngine` + `async_sessionmaker`, registers the psycopg3 dialect for bare `postgresql://` URLs, and verifies connectivity in `open()` (used as an `async with`). Two operations, both taking a mapped model class plus one-or-many instances:
- `insert(records, model, conflict_columns=(), exclude_columns=())` — bulk insert via `pg_insert`; with `conflict_columns` it does `ON CONFLICT (...) DO NOTHING` (idempotent re-scrapes); `exclude_columns` drops server-managed columns from the row dicts.
- `load_unparsed(bronze_model, silver_model, source)` — bronze rows for `source` with no matching `(source, bronze_id)` in silver, ordered by `(created_at, bronze_id)`.

All queries are SQLAlchemy constructs (`select`, `pg_insert`, `.exists()`) — no interpolated SQL.

### `transform/html_parser.py` — `HtmlParser` + helpers · *Pipeline of filters*
`parse(html)` runs ordered passes over a BeautifulSoup tree: strip comments → `decompose` each `blocklist_tags` entry (tag + subtree) → delete each `blocklist_attributes` entry from every remaining tag. Module helpers `compress` / `decompress` (zlib level 6, for `BYTEA` columns) and `compute_content_hash` (SHA-256 of cleaned HTML, for future change-detection dedup).

### `sql/` — `Base` + per-layer `schema.py` · *ORM declarative base + DDL hook*
`Base(DeclarativeBase)` adds `to_dict(exclude_columns)`. Each `sql/<layer>/schema.py` registers a `CREATE SCHEMA IF NOT EXISTS <layer>` listener on `Base.metadata`'s `before_create` event. Importing `sql` (done by `migrations/env.py` and by the model imports in scrapers/parsers) registers every table and hook. Tables:
- **bronze** — two generic capture tables, `bronze.api_snapshots` and `bronze.html_snapshots`, both keyed on the natural key `(source, source_url, created_at)` so the same URL can be re-captured across snapshots.
- **silver** — per-domain normalized entities, `silver.articles` and `silver.products`, each carrying provenance (`bronze_id` + the bronze snapshot's `created_at`).
- **gold** — schema only; aggregated/deduplicated entities are TBD.

### `migrations/env.py` — Alembic environment · *Migration manager*
Registers the psycopg3 dialect (so `DATABASE_URL` stays a plain `postgresql://` — it is not rewritten), `import sql` to populate `target_metadata`, `ensure_schemas()` to `CREATE SCHEMA IF NOT EXISTS` for bronze/silver/gold before any migration runs, `include_name()` to restrict autogenerate to galactus-owned schemas (Airflow shares the database and owns `public`), and a `galactus_alembic_version` table in `public`.

### Patterns at a glance

| Pattern | Where | Role |
|---|---|---|
| Composition root | `core/pipeline.py` `Pipeline` | owns and sequences the stages |
| Strategy | `PipelineStage` impls; concrete scrapers/parsers selected by config path | swap behavior without touching the core |
| Template Method | `extract/base_scraper.py` `BaseScraper.run()`, `transform/base_parser.py` `BaseParser.run()` | fixed lifecycle, a few narrow override hooks |
| Adapter | `extract/stage.py` / `transform/stage.py` / `load/stage.py`; `infra/http.py` `HttpClient` / `HttpResponse` | bridge domain objects & httpx to the pipeline / scraper contracts |
| Repository / data-access | `infra/db.py` `Database` | one configurable persistence gateway (`insert`, `load_unparsed`) |
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
    EX -->|"build_snapshot, insert ON CONFLICT DO NOTHING"| BR{{"bronze.html_snapshots<br/>bronze.api_snapshots<br/>(raw captures, natural key source+url+created_at)"}}
    BR -->|"load_unparsed → decode → build_entities()"| TR[transform]
    TR -->|"insert, stamped with (source, bronze_id, created_at)"| SV{{"silver.articles<br/>silver.products<br/>(normalized entities + provenance)"}}
    SV -.->|"aggregate / dedup across sightings — TBD"| LD[load]
    LD -.-> GD{{"gold.* (TBD)"}}
```

Each source follows the **bronze/silver** medallion shape: capture raw bytes first, parse them into structured rows later.

| Domain & source kind | bronze model | silver model | extract behavior |
|---|---|---|---|
| **noticias** — API sources (`lanacion`, `abc_color`, `hoy`, `latribuna`, `megacadena`) | `ApiSnapshot` | `Article` | paginated JSON feeds; pagination via `seed_urls()` / `next_urls()` overrides |
| **noticias** — HTML sources (`ultimahora`, `npy`, `elnacional`) | `HtmlSnapshot` | `Article` | same-domain BFS, cleaned + zlib-compressed HTML |
| **supermercados** — API sources (`biggie`, `grutter`, `stock`) | `ApiSnapshot` | `Product` | paginated JSON product catalogs |
| **supermercados** — HTML sources (`superseis`, `arete`, `casarica`) | `HtmlSnapshot` | `Product` | same-domain BFS over `/product/`-style URLs |

Design decisions worth knowing:
- **One silver row per (entity, bronze sighting).** Silver does no deduplication; collapsing repeated sightings of the same article/product is reserved for the gold layer (not yet built).
- **Provenance is `(source, bronze_id)`** on every silver row, plus the bronze snapshot's `created_at`.
- **Re-runs are idempotent.** Re-scraping skips duplicate bronze rows via the natural-key `ON CONFLICT DO NOTHING`; re-transforming skips bronze rows already referenced by silver.
- **HTML diff versioning is deferred.** `html_snapshots.is_diff` exists but is always `False` today — every fetch stores full (cleaned) HTML. (And `STORE_HTML_BODY` in `base_scraper.py` is currently `False` while scrapers are being shaken out — flip it back to `True` to store real bodies.)
- **Scheduling and run identity live outside the pipeline.** The CLI takes no `--run-id`; Airflow's metadata DB owns the run ledger.

## Scrapers & parsers

```mermaid
classDiagram
    class BaseScraper {
        <<Template Method>>
        +ClassVar bronze_model
        +ClassVar conflict_columns
        +ClassVar exclude_columns
        +seed_urls() list~str~
        +next_urls(url, response) list~str~
        +build_snapshot(url, response) Base
        +run()
    }
    class BaseParser {
        <<Template Method>>
        +ClassVar bronze_model
        +ClassVar silver_model
        +ClassVar exclude_columns
        +decode(record) Any
        +build_entities(record, decoded) list~Base~
        +run()
    }
    BaseScraper <|-- LanacionScraper : overrides seed_urls/next_urls
    BaseScraper <|-- AbcColorScraper : overrides seed_urls/next_urls
    BaseScraper <|-- UltimahoraScraper : bronze_model = HtmlSnapshot
    BaseScraper <|-- SuperseisScraper : bronze_model = HtmlSnapshot
    BaseParser <|-- LanacionParser : bronze ApiSnapshot / silver Article
    BaseParser <|-- SuperseisParser : bronze HtmlSnapshot / silver Product
```

A minimal HTML scraper is just the class var:

```python
from galactus.extract.base_scraper import BaseScraper
from sql.a_bronze.html_snapshots import HtmlSnapshot


class Scraper(BaseScraper):
    """Scraper for ultimahora — same-domain BFS into bronze.html_snapshots."""

    bronze_model = HtmlSnapshot
```

An API scraper drives pagination through the hooks:

```python
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot


class Scraper(BaseScraper):
    """Scraper for lanacion — Arc Publishing feed, open-ended pagination into bronze.api_snapshots."""

    bronze_model = ApiSnapshot

    def seed_urls(self) -> list[str]:
        return [self._build_url(offset=0)]

    def next_urls(self, url: str, response: HttpResponse) -> list[str]:
        elements = response.json().get("content_elements", [])
        if len(elements) < self.options.page_size:
            return []
        return [self._build_url(offset=self._offset_of(url) + self.options.page_size)]
```

A parser declares its two models and fills in `build_entities()`:

```python
from typing import Any

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.product import Product
from sql.base import Base


class Parser(BaseParser):
    """Parses HtmlSnapshots from superseis.com.py into Product entities."""

    bronze_model = HtmlSnapshot
    silver_model = Product

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        # `decoded` is a BeautifulSoup tree (html_parser already applied the blocklists)
        ...
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
bronze_table: bronze.html_snapshots          # or bronze.api_snapshots
silver_table: silver.products                # or silver.articles
log_level: INFO
extract:
  scraper: supermercados.<source>            # dotted path under galactus.extract.scrapers
  concurrency: 1
  timeout_seconds: 30.0
  user_agent: "Mozilla/5.0 ..."
  options:
    base_url: https://www.<source>.com.py
    scrape_url_patterns:                     # empty = persist everything fetched
      - "/product/"
    ignore_url_patterns:
      - /login
      - /cart
    page_size: 0                             # > 0 for paginated APIs
    max_pages: 10                            # soft cap; 0 = unbounded
transform:
  parser: supermercados.<source>             # dotted path under galactus.transform.parsers
  options: {}                                # blocklist_tags / blocklist_attributes if needed
```

### 2. Scraper — `galactus/extract/scrapers/<domain>/<source>.py`

Export a class **named `Scraper`** (the stage imports that exact name). For an HTML source, set `bronze_model = HtmlSnapshot` and you're done. For an API source, also override `seed_urls()` / `next_urls()` (and optionally `build_snapshot()`). See `scrapers/noticias/lanacion.py` and `scrapers/noticias/abc_color.py`.

### 3. Parser — `galactus/transform/parsers/<domain>/<source>.py`

Export a class **named `Parser`** with `bronze_model`, `silver_model`, and a `build_entities(record, decoded)` implementation. Override `decode()` only for a non-standard bronze model; override `_make_html_parser()` to bake in code-level blocklist defaults.

### 4. Airflow DAG — `airflow/dags/<source>_pipeline.py`

Copy an existing DAG and change `SOURCE` / `SOURCE_TYPE`. Each is two `BashOperator`s, `extract >> transform`, shelling out to `galactus --config configs/<source>.yaml --stage <stage>`.

## Orchestration (Airflow)

`docker-compose.yml` runs the whole stack: `db` (Postgres 16, healthchecked), `airflow-init` (one-shot: `airflow db migrate` + create the `admin`/`admin` user), `galactus-migrate` (one-shot: `alembic upgrade head`), `airflow-scheduler`, and `airflow-webserver` on `http://localhost:8080`. The galactus source, configs, and migrations are **bind-mounted** into the Airflow containers at `/home/airflow/galactus` (the DAGs `cd` there before running the CLI), and `airflow/dags/` is mounted too — so editing a DAG is picked up on the next scheduler scan, no rebuild.

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

`tests/unit/` covers the `Pipeline` composition, config loading/validation, the error hierarchy, and the import graph (it compiles, and `core/` imports nothing from the outer layers — the dependency direction stays one-way). `tests/integration/test_db.py` exercises `Database.insert()` / `load_unparsed()` against a real database.
