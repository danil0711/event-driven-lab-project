import asyncio

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
            print("Получен event:", event)

            event = PaymentEvent.model_validate(event)

            inventory_event = service.proccess(event)

            await producer.send(
                settings.kafka_inventory_topic, inventory_event.model_dump()
            )

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
