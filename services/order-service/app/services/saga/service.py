# app/services/saga/service.py

from sqlalchemy.dialects.postgresql import insert

from app.models.orders import Order, OrderStatus
from app.models.processed_requests import ProcessedEvent
from app.services.saga.schema import PaymentProcessEvent, PaymentType


class SagaService:
    def __init__(self, session):
        self.session = session

    async def process(self, event: PaymentProcessEvent):

        is_new = await self.claim(event.event_id)
        if not is_new:
            return

        order = await self.session.get(Order, event.order_id)
        if not order:
            return

        if event.type == PaymentType.SUCCESS:
            order.status = OrderStatus.PAID.value

        elif event.type == PaymentType.FAILED:
            order.status = OrderStatus.FAILED.value

        await self.session.commit()

    async def claim(self, event_id: str) -> bool:
        stmt = (
            insert(ProcessedEvent)
            .values(event_id=event_id)
            .on_conflict_do_nothing()
            .returning(ProcessedEvent.event_id)
        )

        result = await self.session.execute(stmt)
        return result.scalar_one_or_none() is not None