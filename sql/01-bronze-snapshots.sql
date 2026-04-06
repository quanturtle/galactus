CREATE TABLE IF NOT EXISTS bronze.snapshots (
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT NOT NULL,
    url         TEXT NOT NULL,
    html_blob   BYTEA NOT NULL,
    content_hash TEXT,
    fetch_date  DATE NOT NULL DEFAULT CURRENT_DATE,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parsed_at   TIMESTAMPTZ,
    CONSTRAINT uq_snapshots_source_url_date UNIQUE(source, url, fetch_date)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_unparsed
    ON bronze.snapshots(source) WHERE parsed_at IS NULL;
