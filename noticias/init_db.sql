CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

-- Bronze: raw HTML snapshots for BFS scrapers

CREATE TABLE IF NOT EXISTS bronze.snapshots (
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT NOT NULL,
    url         TEXT NOT NULL,
    html_blob   BYTEA NOT NULL,
    fetch_date  DATE NOT NULL DEFAULT CURRENT_DATE,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parsed_at   TIMESTAMPTZ,
    UNIQUE(source, url, fetch_date)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_unparsed
    ON bronze.snapshots(source) WHERE parsed_at IS NULL;

-- Bronze: raw API responses for API scrapers

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

-- Silver: parsed articles

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
    UNIQUE(source, source_url)
);

CREATE INDEX IF NOT EXISTS idx_silver_source ON silver.articles(source);
CREATE INDEX IF NOT EXISTS idx_silver_published ON silver.articles(published_at);
CREATE INDEX IF NOT EXISTS idx_silver_section ON silver.articles(section);

-- Silver: TF-IDF derived tags

CREATE TABLE IF NOT EXISTS silver.article_tags (
    id                 BIGSERIAL PRIMARY KEY,
    silver_article_id  BIGINT NOT NULL,
    tags               TEXT[] NOT NULL,
    processed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(silver_article_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_tags_article
    ON silver.article_tags(silver_article_id);
CREATE INDEX IF NOT EXISTS idx_silver_tags_gin
    ON silver.article_tags USING gin(tags);

-- Silver: NER-extracted named entities

CREATE TABLE IF NOT EXISTS silver.article_entities (
    id                 BIGSERIAL PRIMARY KEY,
    silver_article_id  BIGINT NOT NULL,
    entity_name        TEXT NOT NULL,
    entity_type        VARCHAR(20) NOT NULL DEFAULT 'PER',
    normalized_name    TEXT,
    confidence         FLOAT,
    mention_count      INTEGER NOT NULL DEFAULT 1,
    method             VARCHAR(20) NOT NULL,
    processed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(silver_article_id, entity_name, method)
);

CREATE INDEX IF NOT EXISTS idx_silver_entities_article
    ON silver.article_entities(silver_article_id);
CREATE INDEX IF NOT EXISTS idx_silver_entities_name
    ON silver.article_entities(normalized_name);
CREATE INDEX IF NOT EXISTS idx_silver_entities_method
    ON silver.article_entities(method);

-- Silver: article images with S3 metadata

CREATE TABLE IF NOT EXISTS silver.article_images (
    id                 BIGSERIAL PRIMARY KEY,
    silver_article_id  BIGINT NOT NULL,
    image_url          TEXT NOT NULL,
    image_role         VARCHAR(20) NOT NULL DEFAULT 'hero',
    ordinal            INTEGER NOT NULL DEFAULT 0,
    s3_bucket          VARCHAR(100),
    s3_key             TEXT,
    content_type       VARCHAR(50),
    file_size_bytes    INTEGER,
    width              INTEGER,
    height             INTEGER,
    content_hash       VARCHAR(64),
    download_status    VARCHAR(20) NOT NULL DEFAULT 'pending',
    download_error     TEXT,
    downloaded_at      TIMESTAMPTZ,
    processed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(silver_article_id, image_url)
);

CREATE INDEX IF NOT EXISTS idx_silver_images_article
    ON silver.article_images(silver_article_id);
CREATE INDEX IF NOT EXISTS idx_silver_images_status
    ON silver.article_images(download_status);
CREATE INDEX IF NOT EXISTS idx_silver_images_hash
    ON silver.article_images(content_hash);
