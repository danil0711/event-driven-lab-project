

import asyncio

from app.core.logger import logger


from app.db import async_session_maker


from app.core.config import settings


from app.infrastructure.kafka.producer import KafkaProducer


from app.services.outbox.service import OutboxPublisher

logger.info('Запуск outbox worker')

async def main():
    kafka = KafkaProducer()

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
