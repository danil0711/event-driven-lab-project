import random

from sqlalchemy.dialects.postgresql import insert

from app.core.logger import logger
from app.models.payments import Payment
from app.models.outbox_events import OutboxEvent
from app.models.processed_event import ProcessedEvent
from app.services.payments.schema import (
    OrderCreatedIntegrationEvent,
    PaymentProcessEvent,
)


class PaymentService:
    def __init__(self, session):
        self.session = session

    async def process(self, event: OrderCreatedIntegrationEvent) -> None:

        is_new = await self.claim_event(event.event_id)

        if not is_new:
            logger.info(f"Duplicate event {event.event_id}")
            return

        event = OrderCreatedIntegrationEvent.model_validate(event)

        # симуляция оплаты
        payment_event = PaymentService._build_payment_event(event)

        payment = Payment(
            order_id=event.order_id,
            status=payment_event.type,
            amount=event.total_amount,
        )
        self.session.add(payment)

        await self._write_outbox(payment_event)

    async def claim_event(self, event_id: str) -> bool:
        stmt = (
            insert(ProcessedEvent)
            .values(event_id=event_id)
            .on_conflict_do_nothing()
            .returning(ProcessedEvent.event_id)
        )

        result = await self.session.execute(stmt)
        return result.scalar_one_or_none() is not None

    def _write_outbox(self, event: PaymentProcessEvent):
        outbox_event = OutboxEvent(
            event_id=event.event_id,
            type=event.type,
            payload=event.model_dump(mode="json"),
        )

        self.session.add(outbox_event)

    @staticmethod
    def _build_payment_event(event: OrderCreatedIntegrationEvent):
        """Используем PaymentProcessEvent для валидации"""
        if random.random() < 0.7:
            payment_event = PaymentProcessEvent(
                event_id=event.event_id,
                type="payment_success",
                order_id=event.order_id,
                reason=None,
                items=event.items,
            )

        else:
            payment_event = PaymentProcessEvent(
                event_id=event.event_id,
                type="payment_failed",
                order_id=event.order_id,
                reason="random_fail",
                items=event.items,
            )

        return payment_event
