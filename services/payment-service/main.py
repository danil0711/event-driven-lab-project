import asyncio

from app.consumer import KafkaConsumer
from app.core.config import get_settings
from app.core.db import async_session_maker
from app.producer import KafkaProducer
from app.services.outbox.service import OutboxPublisher
from app.services.payments.service import PaymentService

settings = get_settings()


async def main():
    print("Старт сервиса")

    consumer = KafkaConsumer(
        topic=settings.kafka_orders_topic,
        group_id="orders-consumer",
    )

    producer = KafkaProducer(settings)

    await consumer.start()
    await producer.start()

    try:
        async for event in consumer.listen():
            print("Получен event:", event["event_id"], event)

            async with async_session_maker() as session:
                service = PaymentService(session)
                try:
                    await service.process(event)
                except Exception as e:
                    print(f"Ошибка обработки event {event['event_id']}: {e}")

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
