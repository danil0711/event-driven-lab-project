# app/services/saga/inventory_saga_service.py

from uuid import uuid4

from sqlalchemy import select

from app.core.logger import logger
from app.models.inventory_processed_event import InventoryProcessedEvent
from sqlalchemy.dialects.postgresql import insert

from app.models.outbox_events import OutboxEvent
from app.models.payments import Payment
from app.services.payments.schema import PaymentProcessEvent, PaymentType
from app.services.saga.inventory.schema import InventoryResponse, InventoryResponseType


class InventorySagaService:
    def __init__(self, session):
        self.session = session

    async def process(self, event: InventoryResponse):
        log = logger.bind(
            event_id=event.event_id,
            order_id=event.order_id,
        )

        log.info("Получен inventory SAGA response")

        if not await self.claim(event.event_id):
            log.info("Дубликат inventory SAGA event")
            return

        result: Payment = await self.session.execute(
            select(Payment).where(Payment.order_id == event.order_id)
        )
        payment = result.scalar_one_or_none()
        if not payment:
            log.warning("Платеж не найден")
            return

        if event.type == InventoryResponseType.INVENTORY_FAILED:
            payment.status = PaymentType.REFUNDED
            refund_event = PaymentProcessEvent(
                event_id=str(uuid4()),
                type=PaymentType.REFUNDED,
                order_id=event.order_id,
                reason="inventory_failed",
                items=[],
            )

            self.session.add(
                OutboxEvent(
                    event_id=refund_event.event_id,
                    type=refund_event.type,
                    payload=refund_event.model_dump(mode="json"),
                )
            )
            log.info('Не удалось зарезверировать товар. Платеж будет возвращен.')

        await self.session.commit()

    async def claim(self, event_id: str) -> bool:
        stmt = (
            insert(InventoryProcessedEvent)
            .values(event_id=event_id)
            .on_conflict_do_nothing()
            .returning(InventoryProcessedEvent.event_id)
        )

        result = await self.session.execute(stmt)
        return result.scalar_one_or_none() is not None
