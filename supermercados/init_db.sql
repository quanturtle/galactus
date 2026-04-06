CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

-- Bronze: raw HTML snapshots for re-parsing

CREATE TABLE IF NOT EXISTS bronze.snapshots (
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT NOT NULL,
    url         TEXT NOT NULL,
    context     JSONB,
    html_blob   BYTEA NOT NULL,
    content_hash TEXT,
    fetch_date  DATE NOT NULL DEFAULT CURRENT_DATE,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parsed_at   TIMESTAMPTZ,
    UNIQUE(source, url, fetch_date)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_unparsed
    ON bronze.snapshots(source) WHERE parsed_at IS NULL;

-- Bronze: raw API responses for re-parsing

CREATE TABLE IF NOT EXISTS bronze.api_responses (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT NOT NULL,
    endpoint      TEXT NOT NULL,
    page_params   JSONB,
    response_blob BYTEA NOT NULL,
    fetch_date    DATE NOT NULL DEFAULT CURRENT_DATE,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parsed_at     TIMESTAMPTZ,
    UNIQUE(source, endpoint, fetch_date)
);

CREATE INDEX IF NOT EXISTS idx_api_responses_unparsed
    ON bronze.api_responses(source) WHERE parsed_at IS NULL;

-- Silver: parsed product data

CREATE TABLE IF NOT EXISTS silver.products (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    url TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    price INTEGER,
    sku TEXT,
    scraped_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source, url, scraped_at)
);

CREATE INDEX IF NOT EXISTS idx_silver_products_source ON silver.products(source);
CREATE INDEX IF NOT EXISTS idx_silver_products_sku ON silver.products(sku);
CREATE INDEX IF NOT EXISTS idx_silver_products_scraped ON silver.products(scraped_at);
