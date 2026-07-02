import asyncio

from app.bootstrap.kafka import start_kafka_with_retry
from app.core.logger import logger
from prometheus_client import start_http_server


from app.db import async_session_maker


from app.core.config import settings


from app.infrastructure.kafka.producer import KafkaProducer


from app.services.outbox.service import OutboxPublisher


async def main():
    logger.info("Запуск outbox worker")
    start_http_server(8001)
    kafka = KafkaProducer()

    await start_kafka_with_retry(kafka)

    await kafka.start()

    try:
        while True:
            async with async_session_maker() as session:
                publisher = OutboxPublisher(
                    session=session,
                    kafka=kafka,
                    topic=settings.kafka_orders_topic,
                )

                await publisher.run_once()

            await asyncio.sleep(1)

    finally:
        await kafka.stop()


if __name__ == "__main__":
    asyncio.run(main())
