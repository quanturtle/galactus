CREATE TABLE IF NOT EXISTS silver.products (
    id          SERIAL PRIMARY KEY,
    source      TEXT NOT NULL,
    url         TEXT NOT NULL,
    name        TEXT NOT NULL,
    description TEXT,
    price       INTEGER,
    sku         TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(source, url)
);

CREATE INDEX IF NOT EXISTS idx_silver_products_source ON silver.products(source);
CREATE INDEX IF NOT EXISTS idx_silver_products_sku    ON silver.products(sku);
