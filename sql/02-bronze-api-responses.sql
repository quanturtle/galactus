CREATE TABLE IF NOT EXISTS bronze.api_responses (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT NOT NULL,
    endpoint      TEXT NOT NULL,
    page_params   JSONB,
    response_blob BYTEA NOT NULL,
    fetch_date    DATE NOT NULL DEFAULT CURRENT_DATE,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parsed_at     TIMESTAMPTZ,
    CONSTRAINT uq_api_responses_source_endpoint_date UNIQUE(source, endpoint, fetch_date)
);

CREATE INDEX IF NOT EXISTS idx_api_responses_unparsed
    ON bronze.api_responses(source) WHERE parsed_at IS NULL;
