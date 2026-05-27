import asyncio

from pydantic import ValidationError

from app.consumer import KafkaConsumer
from app.core.config import get_settings
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

    service = InventoryService()

    try:
        async for event in consumer.listen():
            try:
                event = PaymentEvent.model_validate(event)
            except ValidationError:
                print("Некорректный пейлоад")
                continue

            try:
                inventory_event = service.proccess(event)

                print("Отправка", inventory_event)

                await producer.send(
                    settings.kafka_inventory_topic, inventory_event.model_dump()
                )
            except Exception as e:
                print("FAILED EVENT:", e)

                await producer.send(
                    settings.kafka_inventory_retry_1s_topic,
                    {
                        **event.model_dump(),
                        "retry_count": 1
                    }
                )

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
