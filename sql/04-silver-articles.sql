CREATE TABLE IF NOT EXISTS silver.articles (
    id            BIGSERIAL PRIMARY KEY,
    bronze_id     BIGINT,
    source        VARCHAR(50) NOT NULL,
    source_url    TEXT NOT NULL,
    title         TEXT,
    subtitle      TEXT,
    body          TEXT,
    author        VARCHAR(255),
    published_at  TIMESTAMPTZ,
    section       VARCHAR(100),
    image_url     TEXT,
    word_count    INTEGER,
    keywords      TEXT[],
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_silver_source_url UNIQUE(source, source_url)
);

CREATE INDEX IF NOT EXISTS idx_silver_source    ON silver.articles(source);
CREATE INDEX IF NOT EXISTS idx_silver_published ON silver.articles(published_at);
CREATE INDEX IF NOT EXISTS idx_silver_section   ON silver.articles(section);
