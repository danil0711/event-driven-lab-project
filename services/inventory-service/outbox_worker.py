import asyncio

from app.bootstrap.kafka.ensure_topics import start_kafka_with_retry
from app.core.logger import logger
from app.core.db import async_session_maker
from app.core.config import get_settings
from app.producer import KafkaProducer
from app.service.outbox.service import OutboxPublisher

POLL_INTERVAL = 1

settings = get_settings()


async def outbox_worker():
    logger.info("Outbox worker started")

    kafka = KafkaProducer(settings)

    await start_kafka_with_retry(kafka)

    await kafka.start()

    try:
        while True:
            async with async_session_maker() as session:
                publisher = OutboxPublisher(
                    session=session,
                    kafka=kafka,
                    topic=settings.kafka_inventory_topic,
                )

                await publisher.run_once()

            await asyncio.sleep(POLL_INTERVAL)

    finally:
        print("Outbox worker stopping")
        await kafka.stop()


if __name__ == "__main__":
    asyncio.run(outbox_worker())
