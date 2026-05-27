from app.core.logger import logger
from app.event.schemas.payment import PaymentProcessEvent, PaymentType


async def handle_payments_event(event: PaymentProcessEvent):
    if event.type == PaymentType.SUCCESS:
        logger.info(f"Проведен платеж для {event.order_id}")

    elif event.type == PaymentType.FAILED:
        logger.warning(f"Отклонён платеж для {event.order_id}, причина: {event.reason}")
