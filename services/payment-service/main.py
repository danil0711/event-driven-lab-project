import asyncio

from app.consumer import KafkaConsumer
from app.core.logger import logger
from app.core.bootstrap.kafka import ensure_topics, start_kafka_with_retry
from app.core.config import get_settings
from app.core.db import async_session_maker
from app.producer import KafkaProducer
from app.services.payments.service import PaymentService

settings = get_settings()

logger.info("Старт сервиса payment")


async def main():

    producer = KafkaProducer(settings)

    await start_kafka_with_retry(producer)

    logger.info("Запуск loop")

    await ensure_topics()

    consumer = KafkaConsumer(
        topic=settings.kafka_orders_topic,
        group_id="orders-consumer",
    )

    await consumer.start()
    await producer.start()

    try:
        async for event in consumer.listen():
            async with async_session_maker() as session:
                service = PaymentService(session)
                try:
                    await service.process(event)
                    await session.commit()

                    await consumer.consumer.commit()

                    logger.debug(
                        "Offset committed, event_id={}",
                        event["event_id"],
                    )
                except Exception:
                    await session.rollback()
                    logger.exception(
                        "Ошибка обработки event, event_id={}",
                        event["event_id"],
                    )

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
