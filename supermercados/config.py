from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "SUPERMERCADOS_"}

    log_level: str = "INFO"


settings = Settings()
