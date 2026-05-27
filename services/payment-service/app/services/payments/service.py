import random

from sqlalchemy import select

from app.models.payments import Payment
from app.models.processed_event import ProcessedEvent
from app.services.payments.schema import PaymentProcessEvent


class PaymentService:
    def __init__(self, session):
        self.session = session

    async def process(self, event: dict) -> dict:

        event_id = event["event_id"]

        stmt = select(ProcessedEvent).where(
            ProcessedEvent.event_id == event_id
        )

        query_result = await self.session.execute(stmt)
        already = query_result.scalar_one_or_none()

        if already:
            return None

        # симуляция оплаты
        if random.random() < 0.7:
            payment_event  = PaymentProcessEvent(
                event_id=event["event_id"],
                type="payment_success",
                order_id=event["order_id"],
                reason=None,
                items=event["items"],
            )

        else:
            payment_event  = PaymentProcessEvent(
                event_id=event["event_id"],
                type="payment_failed",
                order_id=event["order_id"],
                reason="random_fail",
                items=event["items"],
            )

        payment = Payment(
            order_id=event["order_id"],
            status=payment_event.type,
            amount=event["total_amount"]
        )

        self.session.add(payment)
        self.session.add(ProcessedEvent(event_id=event_id))
        await self.session.commit()

        return payment_event 
