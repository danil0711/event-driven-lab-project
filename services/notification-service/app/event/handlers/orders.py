from app.core.logger import logger
from app.event.schemas.orders import OrderCreatedEvent


async def handle_orders_event(event: OrderCreatedEvent):
    logger.info(f"Создан заказ: {event.event_id}, на сумму {event.total_amount}")
