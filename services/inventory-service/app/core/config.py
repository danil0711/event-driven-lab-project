

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

KafkaTopic = str


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = Field(
        validation_alias="KAFKA_BOOTSTRAP_SERVERS",
    )

    kafka_payments_topic: KafkaTopic = Field(
        default="payments",
    )

    kafka_inventory_topic: KafkaTopic = Field(
        default="inventory"
    )


    kafka_inventory_retry_1s_topic : KafkaTopic = Field(
        default="inventory-retry-1s"
    )
    kafka_inventory_retry_10s_topic : KafkaTopic = Field(
        default="inventory-retry-10s"
    )
    kafka_inventory_retry_1m_topic : KafkaTopic = Field(
        default="inventory-retry-1m"
    )

    kafka_inventory_dlq_topic: KafkaTopic = Field(
        default="inventory-dlq"
    )

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
