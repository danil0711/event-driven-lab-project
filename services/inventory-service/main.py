import asyncio

from pydantic import ValidationError

from app.consumer import KafkaConsumer
from app.core.config import get_settings
from app.db import SessionLocal
from app.producer import KafkaProducer
from app.service.inventory.schema import PaymentEvent
from app.service.inventory.service import InventoryService

settings = get_settings()


async def main():
    print("Старт сервиса")

    consumer = KafkaConsumer(
        topic=settings.kafka_payments_topic, group_id="payments-consumer"
    )
    producer = KafkaProducer(settings)

    await consumer.start()
    await producer.start()

    try:
        async for event in consumer.listen():
            try:
                event = PaymentEvent.model_validate(event)
            except ValidationError:
                print("Некорректный пейлоад")
                continue

            async with SessionLocal() as session:
                service = InventoryService(session)

                try:
                    inventory_event = await service.proccess(event)

                    if inventory_event is None:
                        continue

                    await producer.send(
                        settings.kafka_inventory_topic, inventory_event.model_dump()
                    )
                except Exception as e:
                    print("FAILED EVENT:", e)

                    await producer.send(
                        settings.kafka_inventory_retry_1s_topic,
                        {**event.model_dump(), "retry_count": 1},
                    )

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
