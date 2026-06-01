import asyncio

from app.consumer import KafkaConsumer
from app.core.logger import logger
from app.core.config import get_settings
from app.db import async_session_maker
from app.services.saga.schema import PaymentProcessEvent
from app.services.saga.service import SagaService


settings = get_settings()


async def saga_consumer():
    consumer = KafkaConsumer(
        topic=settings.kafka_payments_topic,
        group_id="order-saga-consumer",
    )

    print(consumer)

    await consumer.start()

    try:
        async for msg in consumer.listen():
            event = PaymentProcessEvent.model_validate(msg)
            logger.info(f'Saga event: {event.event_id}')

            async with async_session_maker() as session:
                service = SagaService(session)
                await service.process(event)

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(saga_consumer())
