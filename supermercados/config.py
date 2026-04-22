from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "SUPERMERCADOS_"}

    log_level: str = "INFO"

    # Size of each bronze->silver transform chunk.
    chunk_size: int = 500


settings = Settings()
