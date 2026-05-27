import asyncio

from pydantic import ValidationError

from app.consumer import KafkaConsumer
from app.db import SessionLocal
from app.producer import KafkaProducer
from app.core.config import Settings
from app.service.inventory.schema import PaymentEvent
from app.service.inventory.service import InventoryService
from app.service.retry.get_retry_topic import get_retry_topic


async def retry_worker(
    topic: str,
    delay_seconds: int,
    settings: Settings,
):
    consumer = KafkaConsumer(
        topic=topic,
        group_id=f"retry-{topic}",
    )

    producer = KafkaProducer(settings)

    await consumer.start()
    await producer.start()

    try:
        async for event in consumer.listen():

            try:
                # задержка перед обработкой (retry backoff)
                await asyncio.sleep(delay_seconds)

                retry_count = event.get("retry_count", 0)

                # validation
                payment_event = PaymentEvent.model_validate(event)

                async with SessionLocal() as session:
                    service = InventoryService(session)

                    # business logic
                    inventory_event = await service.proccess(payment_event)

                    await producer.send(
                        settings.kafka_inventory_topic,
                        inventory_event.model_dump()
                    )

                    print(f"[{topic}] SUCCESS")

            except ValidationError:
                # мусорные данные → сразу в DLQ
                print(f"[{topic}] VALIDATION ERROR → DLQ")

                await producer.send(
                    settings.kafka_inventory_dlq_topic,
                    event
                )

            except Exception as e:
                # бизнес ошибка → retry дальше
                retry_count = event.get("retry_count", 0) + 1
                event["retry_count"] = retry_count

                print(f"[{topic}] FAILED → retry={retry_count}, err={e}")

                next_topic = get_retry_topic(retry_count, settings)

                await producer.send(next_topic, event)

    finally:
        await consumer.stop()