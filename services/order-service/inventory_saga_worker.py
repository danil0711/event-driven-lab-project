# app/workers/saga/inventory_saga_worker.py

import asyncio

from app.consumer import KafkaConsumer
from app.core.logger import logger
from app.core.config import get_settings
from app.db import async_session_maker
from app.services.saga.inventory.schema import InventoryResponse
from app.services.saga.inventory.service import InventorySagaService


settings = get_settings()


async def inventory_saga_worker():
    logger.info('Payments SAGA worker stating.')
    consumer = KafkaConsumer(
        topic=settings.kafka_inventory_topic,
        group_id="order-saga-inventory",
    )

    await consumer.start()

    try:
        async for msg in consumer.listen():
            event = InventoryResponse.model_validate(msg)

            async with async_session_maker() as session:
                service = InventorySagaService(session)
                await service.process(event)

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(inventory_saga_worker())