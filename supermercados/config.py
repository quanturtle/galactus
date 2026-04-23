from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "SUPERMERCADOS_"}

    log_level: str = "INFO"

    # Size of each bronze->silver transform chunk.
    chunk_size: int = 500

    # S3 / MinIO
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_bucket: str = "supermercados-images"
    s3_region: str = "us-east-1"

    # Image pipeline
    image_download_concurrency: int = 10
    image_download_timeout: int = 30


settings = Settings()
