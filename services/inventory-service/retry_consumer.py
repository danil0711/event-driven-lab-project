import asyncio

from app.core.config import get_settings
from app.service.retry.worker import retry_worker

settings = get_settings()


async def main():
    await asyncio.gather(
        retry_worker(
            settings.kafka_inventory_retry_1s_topic,
            1,
            settings,
        ),
        retry_worker(
            settings.kafka_inventory_retry_10s_topic,
            10,
            settings,
        ),
        retry_worker(
            settings.kafka_inventory_retry_1m_topic,
            60,
            settings,
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())