from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from app.core.types import KafkaTopic


class Settings(BaseSettings):
    postgres_user: str = Field(
        validation_alias="POSTGRES_USER",
    )

    postgres_password: str = Field(
        validation_alias="POSTGRES_PASSWORD",
    )

    postgres_db: str = Field(
        validation_alias="POSTGRES_DB",
    )

    postgres_host: str = Field(
        validation_alias="POSTGRES_HOST",
    )

    postgres_port: int = Field(
        validation_alias="POSTGRES_PORT",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    
    kafka_bootstrap_servers: str = Field(
        validation_alias="KAFKA_BOOTSTRAP_SERVERS",
    )

    kafka_orders_topic: KafkaTopic = Field(
        default="orders",
    )


    @property
    def postgres_dsn(self) -> str:
        return (
            "postgresql+asyncpg://"
            f"{self.postgres_user}:"
            f"{self.postgres_password}@"
            f"{self.postgres_host}:"
            f"{self.postgres_port}/"
            f"{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
