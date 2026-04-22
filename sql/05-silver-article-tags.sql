CREATE TABLE IF NOT EXISTS silver.article_tags (
    id                 BIGSERIAL PRIMARY KEY,
    silver_article_id  BIGINT NOT NULL,
    tags               TEXT[] NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(silver_article_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_tags_article ON silver.article_tags(silver_article_id);
CREATE INDEX IF NOT EXISTS idx_silver_tags_gin     ON silver.article_tags USING gin(tags);
