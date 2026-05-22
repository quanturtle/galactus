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

# 2. Postgres 16 + Airflow (init, scheduler, dag-processor, api-server) + a one-shot `galactus-migrate` service
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

The CLI takes exactly two flags: `--config <path>` (required) and `--stage <name>` (optional; one of `extract`, `transform`, `load`). Schema work goes through `alembic` directly (see [Schema & migrations](#schema--migrations)).

## Project structure

```
galactus/
├── galactus/                       # the package — domain-agnostic pipeline core
│   ├── cli.py                      # entrypoint: parse --config/--stage, validate plugins, build & run Pipeline
│   ├── config.py                   # Pydantic frozen config; load_config() reads YAML + injects DATABASE_URL
│   ├── core/
│   │   ├── pipeline.py             # Pipeline + PipelineStage(ABC) — the composition root
│   │   └── errors.py               # exception hierarchy: PipelineError -> Extract/Transform/Load/Infra/Config
│   ├── extract/
│   │   ├── base_scraper.py         # BaseScraper — async BFS crawler with a small public hook surface
│   │   ├── html_processor.py       # HtmlProcessor — ordered cleaning passes; runs at extract time before bronze insert
│   │   ├── stage.py                # ExtractStage — adapts a Scraper into a PipelineStage
│   │   └── scrapers/{noticias,supermercados}/<source>.py   # per-source Scraper subclasses
│   ├── transform/
│   │   ├── base_parser.py          # BaseParser(ABC) — bronze->silver streaming lifecycle (decode -> build_item -> build_entity -> stamp)
│   │   ├── article_parser.py       # ArticleParser mixin — eight extract_* hooks + build_entity for silver.articles
│   │   ├── product_parser.py       # ProductParser mixin — eight extract_* hooks + build_entity for silver.products
│   │   ├── stage.py                # TransformStage — adapts a Parser into a PipelineStage
│   │   └── parsers/{noticias,supermercados}/<source>.py    # per-source Parser(BaseParser, ArticleParser|ProductParser)
│   ├── load/stage.py               # LoadStage — stub for the future gold-layer aggregation
│   └── infra/
│       ├── db.py                   # Database — async SQLAlchemy engine; insert(), load_visited_requests(), stream_unparsed(); zstd compress/decompress
│       ├── http.py                 # HttpClient / HttpRequest / HttpResponse — httpx wrapper: pooling, retry
│       └── logging.py              # setup_logging()
├── sql/                            # ORM models (the schema source the migrations autogenerate from)
│   ├── base.py                     # Base(DeclarativeBase) with to_dict()
│   ├── a_bronze/                   # snapshot.py (abstract base), api_snapshots.py, html_snapshots.py, schema.py
│   ├── b_silver/                   # article.py, product.py, schema.py — silver: per-domain entities
│   └── c_gold/                     # schema.py only — gold layer is a stub
├── migrations/                     # Alembic (env.py: psycopg3 dialect, multi-schema, autogenerate)
├── configs/<source>.yaml           # one YAML per source
├── airflow/
│   ├── Dockerfile
│   └── dags/<source>_pipeline.py   # one DAG per source: extract >> transform BashOperators
├── docker-compose.yml              # Postgres 16 + airflow-init + galactus-migrate + scheduler + dag-processor + api-server
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
    SC -->|"clean HTML via HtmlProcessor, then insert (skip-if-seen-today)"| BRZ[("bronze schema")]

    TS -->|owns for the run| DB2["Database"]
    TS -->|imports by config path| PR["Parser<br/>(BaseParser + ArticleParser/ProductParser mixin)"]
    PR -->|stream_unparsed| BRZ
    PR -->|insert per bronze record| SLV[("silver schema")]

    LS -.future.-> GLD[("gold schema")]
```

Each module is built around a named pattern; per-class lifecycle details live in docstrings. A compact map:

- **`core/pipeline.py` — `Pipeline` / `PipelineStage`** · *Composition root + Strategy.* Ordered `list[PipelineStage]` plus a `{name: stage}` index; `run(stage_name=None)` runs all stages or one. Construction-time invariants (non-empty, no duplicate names) enforced up front.
- **`config.py` — `PipelineConfig` (frozen Pydantic)** · *Configuration object, read at the edges.* `load_config()` is the single YAML + `DATABASE_URL` read at startup; `extra="forbid"` turns key typos into startup errors. Composes `ExtractConfig` (base URL, URL patterns, pagination, pacing, HTTP knobs) and `TransformConfig` (HTML blocklists, batch size).
- **`cli.py` — `main()` / `validate_plugins()`** · *Fail-fast composition + boundary error handling.* Imports the configured scraper/parser before any I/O; `main()` is the only place that converts exceptions to exit codes (`ConfigError → 2`, `PipelineError → 1`).
- **`core/errors.py`** · *Layered exceptions.*
  ```
  PipelineError
  ├── ExtractError      └── ScraperError      (one source failed to fetch)
  ├── TransformError    └── ParserError       (one source failed to parse)
  ├── LoadError
  ├── ConfigError                              (bad/missing config, unknown plugin)
  └── InfraError ── HttpError, DatabaseError   (I/O adapter failure)
  ```
  Infra adapters raise `HttpError`/`DatabaseError`; plugin code re-raises as `ScraperError`/`ParserError` with source + URL context; stages wrap escapes as their `*Error`; only the CLI catches.
- **`extract|transform|load/stage.py` — `*Stage`** · *Adapter.* Each stage opens the infra context managers it needs (`HttpClient` + `Database` for extract; `Database` for transform), `importlib`-resolves the configured plugin (`galactus.extract.scrapers.<dotted.path>`), instantiates it, awaits its `run()`, and re-raises failures as the stage's error type. `LoadStage` is a no-op stub today.
- **`extract/base_scraper.py` — `BaseScraper`** · *Template Method.* Fixes the crawl lifecycle: pre-load `seen` via `seen_today()`, BFS the frontier with spawn-and-drain bounded by `concurrency` and `max_pages` (hard cap, counted at spawn), per-URL `process_response` → `should_persist` → `extract_body` → `Database.insert`, then `get_next_urls` folded back via `should_enqueue`. HTML responses are parsed once with `self.html_processor` when present (`None` on API scrapers) and the soup is reused for cleaning and link extraction. Per-URL try/except so one fetch/persist/next-url failure logs and skips instead of aborting the source. Hook surface: `seed_urls`, `fetch`, `extract_links`, `build_url`, `get_next_urls`, `should_enqueue`, `should_persist`, `extract_body`, `process_response`, plus `http_extras` / `db_extras` / `make_html_processor`. Defaults are keyed on `bronze_model` (`HtmlSnapshot` ⇒ scrape every `<a href>`, build an `HtmlProcessor`, and store zstd-compressed cleaned HTML; `ApiSnapshot` ⇒ no processor, store the zstd-compressed raw body). `build_url(url, params=None)` canonicalizes outgoing requests (lowercase scheme + host, strip `TRACKING_PARAMS`, drop fragment); paginated APIs override with a pagination signature called from their own `seed_urls()` / `get_next_urls()` — keep the `url=` / `params=` keyword path so `seen_today` can re-hash captured requests. There is no separate `BaseApiScraper`.
- **`extract/html_processor.py` — `HtmlProcessor`** · *Pipeline of filters.* `parse(text)` builds an `lxml` soup; `clean(soup)` is async (`asyncio.to_thread`, so the asyncio loop is not blocked while CPU-heavy cleaning runs) and applies three ordered passes — strip comments, decompose `blocklist_tags` (tag + subtree), strip `blocklist_attributes` from every remaining tag. `BASELINE_BLOCKLIST_TAGS = ("script", "style", "noscript")` is always applied; `<script type="application/ld+json">` is always preserved so source parsers can read structured data from it. **Cleaning runs at extract time**, so bronze stores already-cleaned HTML.
- **`transform/base_parser.py` — `BaseParser`** · *Template Method.* `run()` streams `Database.stream_unparsed(...)` and per record runs `decode → build_item → build_entity → stamp`, inserting silver per record. `decode()` defaults: `HtmlSnapshot` → `BeautifulSoup(self.db.decompress(record.body), "lxml")`; `ApiSnapshot` → `json.loads(self.db.decompress(record.body))`. `build_item(decoded)` defaults to `[decoded]` — override only for listing-style payloads. `stamp(entity, record)` carries `(bronze_id, created_at)`. A decode/build failure logs and skips the row; silver does not commit, so the next run retries it through `stream_unparsed`. Concrete parsers must set `silver_model` and mix in `ArticleParser` or `ProductParser`; `bronze_model` defaults to `HtmlSnapshot`. No dedup here — one silver row per `(entity, bronze sighting)`; collapsing across sightings is the gold layer's job.
- **`transform/article_parser.py`, `transform/product_parser.py`** · *Mixin (role contribution).* Each owns `build_entity(item) → Article | Product` and declares eight abstract `extract_*` hooks in silver-column order so a parser file reads top-to-bottom against the schema. Every silver field is optional, so the hooks return whatever they can find and `build_entity` does not filter. `ArticleParser` hooks: `source_url`, `title`, `body`, `authors`, `published_at`, `section`, `tags`, `image_urls`. `ProductParser` hooks: `source_url`, `sku`, `name`, `brand`, `price`, `currency`, `unit`, `image_urls` — plus `parse_unit_from_name(name)`, an ordered regex list (kg, l, ml, g, cc) for the inline unit info embedded in most supermercado product names.
- **`infra/http.py` — `HttpClient` / `HttpRequest` / `HttpResponse`** · *Adapter.* `httpx.AsyncClient` wrapper with pooling and `follow_redirects=True`; fetch concurrency lives in `BaseScraper.run`, not here. `get(request)` is single-attempt — returns any `< 500` response and raises `HttpError` on 5xx/transport failures (connect errors, timeouts, mid-stream disconnects); `BaseScraper.run` turns that into a per-URL skip and same-day reruns self-heal via `seen_today`. `HttpRequest` is a hashable value object — `hash(request)` is the BFS `seen` key. `HttpResponse` exposes only `status_code`/`headers`/`content`/`text`/`json()`/`request` — scrapers never touch `httpx` directly.
- **`infra/db.py` — `Database`** · *Repository / data-access gateway.* One configurable class. Owns a single `AsyncEngine` + `async_sessionmaker`, registers the psycopg3 dialect for bare `postgresql://` URLs, verifies connectivity in `open()` (used as `async with`). API: `insert(records, model)` — bulk insert via SQLAlchemy `insert`; columns `None` on every row are dropped so the database applies its own defaults; no `ON CONFLICT` (same-day re-fetch dedup lives in `BaseScraper.seen_today`). `load_visited_requests(model, source)` — `(request_url, request_params)` tuples captured (2xx only) since UTC midnight; powers `seen_today`. `stream_unparsed(bronze_model, silver_model, source, chunk_size=100)` — async generator yielding bronze rows with no matching `(source, bronze_id)` in silver, ordered by `(created_at, id)`, server-streamed via `yield_per`. `compress(text) → bytes` / `decompress(blob) → str` — zstd level 6 for `BYTEA` columns; scrapers `compress` on the way in, parsers `decompress` on the way out. All queries are SQLAlchemy constructs (`select`, `insert`, `.exists()`) — no interpolated SQL.
- **`sql/` — `Base` + per-layer `schema.py`, `a_bronze/snapshot.py`** · *ORM declarative base + DDL hook.* `Base.to_dict()` materializes per-row dicts for `Database.insert`. Each `sql/<layer>/schema.py` registers a `CREATE SCHEMA IF NOT EXISTS <layer>` listener on `Base.metadata.before_create`. **Bronze is a single shape**: `Snapshot` is an abstract base (id, source, request_url, request_headers, request_params, status_code, response_headers, content_type, body, created_at); `HtmlSnapshot` and `ApiSnapshot` are thin subclasses that only set `__tablename__`. Silver carries provenance (`bronze_id` → bronze `id`, plus the bronze snapshot's `created_at` stamped at parse time). Gold is a schema-only stub.
- **`extract/scrapers/<domain>/<source>.py`, `transform/parsers/<domain>/<source>.py`** · *Strategy / plugin.* Each module exports a single `Scraper` or `Parser` class. Selection is by dotted path in YAML — `extract.scraper: noticias.lanacion` resolves to `galactus.extract.scrapers.noticias.lanacion.Scraper`. No registry; the CLI imports the path and checks the class is there. HTML scrapers are typically one-liners (`bronze_model = HtmlSnapshot`); API scrapers set `bronze_model = ApiSnapshot` and override `seed_urls` / `get_next_urls` / `build_url`. Parsers implement the eight `extract_*` hooks from the mixin.
- **`migrations/env.py`** · *Migration manager.* Registers the psycopg3 dialect (so `DATABASE_URL` stays plain `postgresql://`), `import sql` to populate `target_metadata`, `ensure_schemas()` to `CREATE SCHEMA IF NOT EXISTS` bronze/silver/gold before any migration, `include_name()` to restrict autogenerate to galactus-owned schemas (Airflow shares the DB and owns `public`), and a `galactus_alembic_version` table in `public`.

### Patterns at a glance

| Pattern | Where | Role |
|---|---|---|
| Composition root | `core/pipeline.py` `Pipeline` | owns and sequences the stages |
| Strategy | `PipelineStage` impls; concrete scrapers/parsers selected by config path | swap behavior without touching the core |
| Template Method | `extract/base_scraper.py` `BaseScraper.run()` (spawn-and-drain BFS), `transform/base_parser.py` `BaseParser.run()` (streaming bronze→silver, per-record insert) | fixed lifecycle, a few narrow override hooks |
| Mixin (role contribution) | `transform/article_parser.py` `ArticleParser`, `transform/product_parser.py` `ProductParser` | contribute `build_entity` + eight `extract_*` hooks per silver entity, composed with `BaseParser` via MRO |
| Adapter | `extract/stage.py` / `transform/stage.py` / `load/stage.py`; `infra/http.py` `HttpClient` / `HttpRequest` / `HttpResponse` | bridge domain objects & httpx to the pipeline / scraper contracts |
| Repository / data-access | `infra/db.py` `Database` | one configurable persistence gateway (`insert`, `load_visited_requests`, `stream_unparsed`; zstd `compress`/`decompress`) |
| Pipeline of filters | `extract/html_processor.py` `HtmlProcessor` | ordered HTML-cleaning passes, applied at extract time before bronze insert |
| Configuration object (edges-only) | `config.py` `PipelineConfig` + `load_config()` | one typed, frozen read at startup |
| Layered exception hierarchy | `core/errors.py` | categorize failures by layer; translate to exit codes only at the CLI boundary |
| ORM declarative base + DDL hook | `sql/base.py`, `sql/*/schema.py`; `sql/a_bronze/snapshot.py` | shared model base; abstract `Snapshot` is the bronze shape; auto-create the layer schemas |
| Migration manager | `migrations/env.py` | versioned, multi-schema, psycopg3, autogenerated migrations |
| Plugin discovery / fail-fast | `cli.py` `validate_plugins()` | import + validate the configured source modules before running |

## Data pipeline

```mermaid
flowchart LR
    SRC[("website / API")] -->|"Scraper.run() — BFS / paginated fetch"| EX[extract]
    EX -->|"HtmlProcessor.clean (HTML only), insert (skip-if-seen-today)"| BR{{"bronze.html_snapshots<br/>bronze.api_snapshots<br/>(raw captures, id PK + zstd-compressed body)"}}
    BR -->|"stream_unparsed → decode → build_item → build_entity"| TR[transform]
    TR -->|"insert per record, stamped with (source, bronze_id, created_at)"| SV{{"silver.articles<br/>silver.products<br/>(normalized entities + provenance)"}}
    SV -.->|"aggregate / dedup across sightings — TBD"| LD[load]
    LD -.-> GD{{"gold.* (TBD)"}}
```

Each source follows the **bronze/silver** medallion shape: capture raw bytes first, parse them into structured rows later.

| Domain & source kind | bronze model | silver model | extract behavior |
|---|---|---|---|
| **noticias** — API sources (e.g. `lanacion`, `abc_color`) | `ApiSnapshot` | `Article` | paginated JSON feeds; pagination via `seed_urls()` / `get_next_urls()` overrides |
| **noticias** — HTML sources (e.g. `ultimahora`, `hoy`, `latribuna`) | `HtmlSnapshot` | `Article` | same-domain BFS, zstd-compressed cleaned HTML body |
| **supermercados** — API sources (e.g. `biggie`, `grutter`) | `ApiSnapshot` | `Product` | paginated JSON product catalogs |
| **supermercados** — HTML sources (e.g. `superseis`, `losjardines`, `casarica`) | `HtmlSnapshot` | `Product` | same-domain BFS over product URLs |

Current catalog (15 sources): `noticias/{abc_color, elnacional, hoy, lanacion, latribuna, megacadena, npy, ultimahora}` and `supermercados/{arete, biggie, casarica, grutter, losjardines, stock, superseis}`. Each has a matching scraper, parser, YAML config, and Airflow DAG.

Design decisions worth knowing:
- **Bronze is a single shape.** `sql/a_bronze/snapshot.py` defines an abstract `Snapshot` (id, source, request_url/headers/params, status_code, response_headers, content_type, body, created_at). `HtmlSnapshot` and `ApiSnapshot` are thin subclasses that only set `__tablename__`; HTML cleaning happens at extract time before the body is compressed and stored.
- **One silver row per (entity, bronze sighting).** Silver does no deduplication; collapsing repeated sightings of the same article/product is reserved for the gold layer (not yet built).
- **Provenance is `(source, bronze_id)`** on every silver row, plus the bronze snapshot's `created_at`.
- **Same-day re-runs are idempotent.** `seen_today()` pre-loads the BFS `seen` set from bronze rows captured (2xx) since UTC midnight, so re-scraping today re-fetches the seeds (to discover new content) but skips any request already in bronze; re-transforming skips bronze rows already referenced by silver. Re-runs on a later day re-fetch — each calendar day produces its own snapshot in bronze.
- **Scheduling and run identity live outside the pipeline.** The CLI takes no `--run-id`; Airflow's metadata DB owns the run ledger.

## Known data peculiarities

Real-world capture artifacts that are *correct* bronze/silver behavior, not pipeline bugs:

- **hoy — epoch publish dates.** ~12,054 `silver.articles` rows for `hoy` carry a `published_at` of `1970-01-01` (stored as `1970-01-01 04:00:00`, the Unix epoch in Paraguay's −04:00 offset). Confirmed against bronze: hoy.com.py's WordPress API itself returns the epoch in *every* date field (`date`, `date_gmt`, `modified`, `modified_gmt`) for these posts, so `extract_published_at` (which reads `date_gmt or date`) faithfully parses what it was given — there is no real date to recover from bronze. The affected posts cluster under the site's `radio-970` section.
- **arete / casarica / grutter — no structured brand.** Unlike `losjardines` (whose Dattamax breadcrumb carries a `marca` crumb), these three sources expose no brand field anywhere in the page or API payload — the brand is only embedded in the product `name`. Their `extract_brand` returns `None`; populating brand for them requires deriving it from `name`.

## Scrapers & parsers

```mermaid
classDiagram
    class BaseScraper {
        <<Template Method>>
        +ClassVar bronze_model
        +seed_urls() list~HttpRequest~
        +fetch(request) HttpResponse
        +extract_links(response, soup) list~str~
        +build_url(url, params=None) HttpRequest
        +get_next_urls(response, soup) list~HttpRequest~
        +should_enqueue(request) bool
        +should_persist(request) bool
        +extract_body(response, soup) str
        +process_response(response) list~HttpRequest~
        +make_html_processor() HtmlProcessor | None
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

A minimal HTML scraper is just the class var — defaults handle BFS, cleaning, link extraction, and zstd storage:

```python
from galactus.extract.base_scraper import BaseScraper
from sql.a_bronze.html_snapshots import HtmlSnapshot


class Scraper(BaseScraper):
    """Scraper for ultimahora — same-domain BFS into bronze.html_snapshots."""

    bronze_model = HtmlSnapshot
```

For paginated API sources, set `bronze_model = ApiSnapshot` and override `seed_urls()` / `get_next_urls()` with a paginating `build_url(...)` — keep the `url=` / `params=` keyword path on the signature so `seen_today()` can re-hash captured requests. Working examples: `scrapers/noticias/lanacion.py` (offset-paginated Arc feed) and `scrapers/noticias/abc_color.py` (per-section paginated).

Parsers compose `BaseParser` with `ArticleParser` or `ProductParser` and implement the eight `extract_*` hooks against the decoded payload. Override `build_item(decoded)` when one bronze record carries many entities (listing-style payloads); override `decode()` to bundle bronze context (e.g. `record.request_url`) into the per-entity `item`. Working examples: `parsers/supermercados/superseis.py` reads JSON-LD Product on every page; `parsers/noticias/lanacion.py` builds many `Article` items per bronze record via `build_item` over an Arc PF feed.

## Schema & migrations

**Alembic is the single source of truth, and migrations are autogenerated — never hand-written.** The workflow is: edit the SQLAlchemy models under `sql/`, then

```bash
uv run alembic revision --autogenerate -m "describe the change"   # generate the migration from model changes
uv run alembic upgrade head                                       # apply it
uv run alembic current                                            # show the applied revision
uv run alembic history                                            # show the revision graph
uv run alembic downgrade -1                                        # roll back one step
```

`migrations/env.py` registers the psycopg3 dialect (DATABASE_URL stays `postgresql://`, no rewriting), creates the `bronze` / `silver` / `gold` schemas before migrating, restricts autogenerate to those schemas (Airflow shares the DB and owns `public`), and tracks state in a `galactus_alembic_version` table in `public`. Under Docker, the one-shot `galactus-migrate` service runs `alembic upgrade head` before the scheduler/api-server start.

## Adding a new source

A *source* is one website or API within a domain (`noticias` or `supermercados`). Four files, one for each:

1. **YAML config** — `configs/<source>.yaml`. Copy a sibling. Sets `extract.scraper`, `transform.parser`, `base_url`, `allowed_domains`, `scrape_patterns`, `ignore_patterns`, `max_pages`, `concurrency`, `timeout_seconds`, and the HTML blocklists. Paginated API sources hard-code their page size on the scraper class (e.g. `LIMIT = 100`) since each API encodes it differently under its own query blob.
2. **Scraper** — `galactus/extract/scrapers/<domain>/<source>.py`, exporting a class named `Scraper`. HTML source: subclass `BaseScraper` and set `bronze_model = HtmlSnapshot` (see `scrapers/noticias/ultimahora.py`). API source: also override `seed_urls()` / `get_next_urls()` / `build_url(...)` — keep `url=` / `params=` on the signature so `seen_today()` can re-hash captured requests (see `scrapers/noticias/lanacion.py`).
3. **Parser** — `galactus/transform/parsers/<domain>/<source>.py`, exporting a class named `Parser` that subclasses **both** `BaseParser` and one of `ArticleParser` / `ProductParser`. Set `silver_model` (and `bronze_model` if not `HtmlSnapshot`); implement the eight `extract_*` hooks. Override `build_item(decoded)` for listing-style payloads, and `decode()` to bundle bronze context (e.g. `record.request_url`) into the per-entity `item`.
4. **Airflow DAG** — `airflow/dags/<source>_pipeline.py`. Copy a sibling and change `SOURCE` / `SOURCE_TYPE`; tasks are two `BashOperator`s, `extract >> transform`, shelling out to `galactus --config configs/<source>.yaml --stage <stage>`.

## Orchestration (Airflow)

`docker-compose.yml` runs the whole stack: `db` (Postgres 16, healthchecked), `airflow-init` (one-shot: `airflow db migrate` + create the `admin`/`admin` user), `galactus-migrate` (one-shot: invokes `/home/airflow/galactus/.venv/bin/alembic upgrade head` directly), `airflow-scheduler`, `airflow-dag-processor` (parses DAG files — a standalone component in Airflow 3), and `airflow-apiserver` on `http://localhost:8080`. The galactus source, configs, and migrations are **bind-mounted** into the Airflow containers at `/home/airflow/galactus` (the DAGs `cd` there before running the CLI), and `airflow/dags/` is mounted too — so editing a DAG is picked up on the next scheduler scan, no rebuild.

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
