# app/services/saga/service.py

from sqlalchemy.dialects.postgresql import insert

from app.core.logger import logger
from app.models.orders import Order, OrderStatus
from app.models.processed_requests import ProcessedEvent
from app.services.saga.payments.schema import PaymentProcessEvent, PaymentType


class SagaService:
    def __init__(self, session):
        self.session = session

    async def process(self, event: PaymentProcessEvent):

        log = logger.bind(
            event_id=event.event_id,
            order_id=event.order_id,
        )

        log.info("Получен payment event")

        is_new = await self.claim(event.event_id)
        if not is_new:
            log.info("Дубликат payment event")
            return

        order = await self.session.get(Order, event.order_id)
        if not order:
            log.info("Заказ не найден")
            return

        if event.type == PaymentType.SUCCESS:
            order.status = OrderStatus.PAID.value

            log.info("Заказ помечен как оплаченный, status={}", OrderStatus.PAID)

        elif event.type == PaymentType.FAILED:

            log.info("Заказ не удалось оплатить, status={}", OrderStatus.FAILED)
            order.status = OrderStatus.FAILED.value

        await self.session.commit()

        log.info(
            "Статус заказа обновлен, status={}",
            order.status,
        )

    async def claim(self, event_id: str) -> bool:
        stmt = (
            insert(ProcessedEvent)
            .values(event_id=event_id)
            .on_conflict_do_nothing()
            .returning(ProcessedEvent.event_id)
        )

        result = await self.session.execute(stmt)
        return result.scalar_one_or_none() is not None