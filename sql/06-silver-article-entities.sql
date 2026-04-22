CREATE TABLE IF NOT EXISTS silver.article_entities (
    id                 BIGSERIAL PRIMARY KEY,
    silver_article_id  BIGINT NOT NULL,
    entity_name        TEXT NOT NULL,
    entity_type        VARCHAR(20) NOT NULL DEFAULT 'PER',
    normalized_name    TEXT,
    confidence         FLOAT,
    mention_count      INTEGER NOT NULL DEFAULT 1,
    method             VARCHAR(20) NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(silver_article_id, entity_name, method)
);

CREATE INDEX IF NOT EXISTS idx_silver_entities_article ON silver.article_entities(silver_article_id);
CREATE INDEX IF NOT EXISTS idx_silver_entities_name    ON silver.article_entities(normalized_name);
CREATE INDEX IF NOT EXISTS idx_silver_entities_method  ON silver.article_entities(method);
