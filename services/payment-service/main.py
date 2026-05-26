import asyncio

from app.consumer import KafkaConsumer
from app.core.config import get_settings
from app.producer import KafkaProducer
from app.services.payments.schema import PaymentService

settings = get_settings()


async def main():
    print("Старт сервиса")
    consumer = KafkaConsumer(
        topic=settings.kafka_orders_topic,
        group_id="orders-consumer",
    )

    producer = KafkaProducer(settings)
    service = PaymentService()

    await consumer.start()
    await producer.start()

    try:
        async for event in consumer.listen():
            print('Получен event:', event['event_id'])
            payment_event = service.process(event)
            print('payment_event:', payment_event)

            await producer.send(
                settings.kafka_payments_topic, payment_event.model_dump()
            )

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
