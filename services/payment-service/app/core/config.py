from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

KafkaTopic = str


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = Field(
        validation_alias="KAFKA_BOOTSTRAP_SERVERS",
    )

    kafka_orders_topic: KafkaTopic = Field(
        default="orders",
    )

    kafka_payments_topic: KafkaTopic = Field(default="payments")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
