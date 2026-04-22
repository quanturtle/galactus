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
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_silver_article_image UNIQUE(silver_article_id, image_url)
);

CREATE INDEX IF NOT EXISTS idx_silver_images_article ON silver.article_images(silver_article_id);
CREATE INDEX IF NOT EXISTS idx_silver_images_status  ON silver.article_images(download_status);
CREATE INDEX IF NOT EXISTS idx_silver_images_hash    ON silver.article_images(content_hash);
