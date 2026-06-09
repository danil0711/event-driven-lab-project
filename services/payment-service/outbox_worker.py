import asyncio

from app.core.bootstrap.kafka import start_kafka_with_retry
from app.core.logger import logger
from app.core.config import get_settings
from app.core.db import async_session_maker
from app.producer import KafkaProducer
from app.services.outbox.service import OutboxPublisher

settings = get_settings()


async def main():
    logger.info("Outbox worker старт")

    producer = KafkaProducer(settings)
    await start_kafka_with_retry(producer)
    await producer.start()

    try:
        while True:
            async with async_session_maker() as session:
                worker = OutboxPublisher(
                    session,
                    producer,
                    settings.kafka_payments_topic
                )

                await worker.run_once()

            await asyncio.sleep(1)

    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())