from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "NOTICIAS_"}

    database_url: str = "postgresql://the_scraper:the_scraper_secret@localhost:5432/the_scraper"
    log_level: str = "INFO"

    # MLX local LLM server
    mlx_url: str = "http://localhost:8081"
    mlx_model: str = "mlx-community/gemma-3-4b-it-qat-4bit"
    llm_timeout: int = 60

    # S3 / MinIO
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "noticias"
    s3_secret_key: str = "noticias123"
    s3_bucket: str = "noticias-images"
    s3_region: str = "us-east-1"

    # Image pipeline
    image_download_concurrency: int = 10
    image_download_timeout: int = 30


settings = Settings()
