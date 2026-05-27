import asyncio

from app.consumer import KafkaConsumer
from app.core.config import get_settings
from app.db import SessionLocal
from app.producer import KafkaProducer
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
            print('Получен event:', event['event_id'], event)

            async with SessionLocal() as session:
                service = PaymentService(session)

                payment_event = await service.process(event)

                if payment_event is None: 
                    continue

                print('payment_event:', payment_event)

                await producer.send(
                    settings.kafka_payments_topic, payment_event.model_dump()
                )

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
