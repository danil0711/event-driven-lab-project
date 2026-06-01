# app/services/saga/inventory_saga_service.py

from app.models.inventory_processed_event import InventoryProcessedEvent
from app.models.orders import Order, OrderStatus
from sqlalchemy.dialects.postgresql import insert

from app.services.saga.inventory.schema import InventoryResponse, InventoryResponseType


class InventorySagaService:
    def __init__(self, session):
        self.session = session

    async def process(self, event: InventoryResponse):
        if not await self.claim(event.event_id):
            return

        order = await self.session.get(Order, event.order_id)
        if not order:
            return
        
        print(event.type)

        if event.type == InventoryResponseType.INVENTORY_RESERVED:
            order.status = OrderStatus.COMPLETED.value

        elif event.type == InventoryResponseType.INVENTORY_FAILED:
            order.status = OrderStatus.CANCELLED.value

        elif event.type == InventoryResponseType.INVENTORY_SKIPPED:
            order.status = OrderStatus.CANCELLED.value

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
