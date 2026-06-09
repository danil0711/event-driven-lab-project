import asyncio

from pydantic import ValidationError

from app.bootstrap.kafka.ensure_topics import ensure_topics, start_kafka_with_retry
from app.consumer import KafkaConsumer
from app.core.logger import logger
from app.core.config import get_settings
from app.db import SessionLocal
from app.producer import KafkaProducer
from app.service.inventory.schema import PaymentEvent
from app.service.inventory.service import InventoryService

settings = get_settings()


async def main():
    print("Старт сервиса")

    producer = KafkaProducer(settings)

    await start_kafka_with_retry(producer)
    await ensure_topics()

    logger.info("Запуск loop")

    consumer = KafkaConsumer(
        topic=settings.kafka_payments_topic, group_id="payments-consumer"
    )

    await consumer.start()
    await producer.start()

    try:
        async for event in consumer.listen():
            try:
                event = PaymentEvent.model_validate(event)
            except ValidationError:
                print("Некорректный пейлоад")
                continue

            try:
                async with SessionLocal() as session:
                    service = InventoryService(session)

                    await service.process(event)
                    await session.commit()

                    logger.debug(
                        "Offset committed, event_id={}",
                        event.event_id,
                    )

                    await consumer.consumer.commit()

            except Exception:
                logger.exception(
                    "Ошибка обработки event, event_id={}",
                    event.event_id,
                )

                # rollback уже внутри service или здесь
                # НИЧЕГО НЕ КОММИТИМ В KAFKA

                continue

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
