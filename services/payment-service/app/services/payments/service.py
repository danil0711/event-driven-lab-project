import random

from sqlalchemy.exc import SQLAlchemyError

from app.models.outbox_events import OutboxEvent
from app.models.payments import Payment
from app.services.payments.schema import (
    OrderCreatedIntegrationEvent,
    PaymentProcessEvent,
)


class PaymentService:
    def __init__(self, session):
        self.session = session

    async def process(self, event: OrderCreatedIntegrationEvent) -> None:

        event = OrderCreatedIntegrationEvent.model_validate(event)

        # симуляция оплаты
        payment_event = PaymentService._build_payment_event(event)

        payment = Payment(
            order_id=event.order_id,
            status=payment_event.type,
            amount=event.total_amount,
        )

        outbox_event = OutboxEvent(
            event_id=payment_event.event_id,
            type=payment_event.type,
            payload=payment_event.model_dump(mode="json"),
        )

        self.session.add(payment)
        self.session.add(outbox_event)

        try:
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            raise

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
