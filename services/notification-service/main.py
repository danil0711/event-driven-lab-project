import asyncio

from pydantic import ValidationError

from app.consumers.inventory import KafkaConsumer
from app.core.logger import logger
from app.core.config import get_settings
from app.event.handlers.inventory import handle_inventory_event
from app.event.handlers.orders import handle_orders_event
from app.event.handlers.payments import handle_payments_event
from app.event.schemas.inventory import InventoryResponse
from app.event.schemas.orders import OrderCreatedEvent
from app.event.schemas.payment import PaymentProcessEvent

settings = get_settings()


async def inventory_loop(consumer: KafkaConsumer):
    async for event in consumer.listen():
        try:
            event = InventoryResponse.model_validate(event)
        except ValidationError:
            logger.error('Неккоректное сообщение в топике inventory')
        await handle_inventory_event(event)


async def payments_loop(consumer: KafkaConsumer):
    async for event in consumer.listen():
        try:
            event = PaymentProcessEvent.model_validate(event)
        except ValidationError:
            logger.error('Неккоректное сообщение в топике orders')

        await handle_payments_event(event)


async def orders_loop(consumer: KafkaConsumer):
    async for event in consumer.listen():
        try:
            event = OrderCreatedEvent.model_validate(event)
        except ValidationError:
            logger.error('Неккоректное сообщение в топике orders')
        await handle_orders_event(event)


async def main():
    print("Старт сервиса")

    inventory_consumer = KafkaConsumer(
        topic=settings.kafka_inventory_topic,
        group_id="notification-inventory-consumer",
    )

    payments_consumer = KafkaConsumer(
        topic=settings.kafka_payments_topic,
        group_id="notification-payments-consumer",
    )

    orders_consumer = KafkaConsumer(
        topic=settings.kafka_orders_topic,
        group_id="notification-orders-consumer",
    )

    await asyncio.gather(
        inventory_consumer.start(),
        payments_consumer.start(),
        orders_consumer.start(),
    )

    try:
        await asyncio.gather(
            inventory_loop(inventory_consumer),
            payments_loop(payments_consumer),
            orders_loop(orders_consumer),
        )

    finally:
        await asyncio.gather(
            inventory_consumer.stop(),
            payments_consumer.stop(),
            orders_consumer.stop(),
        )


if __name__ == "__main__":
    asyncio.run(main())
