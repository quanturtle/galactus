CREATE TABLE IF NOT EXISTS silver.products (
    id         SERIAL PRIMARY KEY,
    source     TEXT NOT NULL,
    url        TEXT NOT NULL,
    name       TEXT NOT NULL,
    description TEXT,
    price      INTEGER,
    sku        TEXT,
    scraped_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source, url, scraped_at)
);

CREATE INDEX IF NOT EXISTS idx_silver_products_source  ON silver.products(source);
CREATE INDEX IF NOT EXISTS idx_silver_products_sku     ON silver.products(sku);
CREATE INDEX IF NOT EXISTS idx_silver_products_scraped ON silver.products(scraped_at);
