# Остатки товаров
import random

from sqlalchemy import select

from app.models.processed_event import ProcessedEvent
from app.service.inventory.schema import (
    InventoryResponse,
    InventoryResponseType,
    PaymentEvent,
    PaymentType,
)


STOCK = {10: 50, 20: 100}


class InventoryService:
    def __init__(self, session):
        self.session = session

    async def proccess(self, event: PaymentEvent) -> InventoryResponse:
        if random.random() < 0.5:
            raise Exception("Inventory service crashed")

        event_id = event.event_id

        if event.type != PaymentType.SUCCESS:
            return InventoryResponse(
                event_id=event_id,
                type=InventoryResponseType.INVENTORY_SKIPPED,
                order_id=event.order_id,
            )

        stmt = select(ProcessedEvent).where(ProcessedEvent.event_id == event_id)

        query_result = await self.session.execute(stmt)
        already = query_result.scalar_one_or_none()

        if already:
            print(f'Дублирующее событие: {event_id}, пропуск')
            return None

        for item in event.items:
            available = STOCK.get(item.product_id)

            if available < item.quantity:
                return InventoryResponse(
                    event_id=event_id,
                    type=InventoryResponseType.INVENTORY_FAILED,
                    order_id=event.order_id,
                    reason="out_of_stock",
                    product_id=item.product_id,
                )

        for item in event.items:
            STOCK[item.product_id] -= item.quantity

        self.session.add(ProcessedEvent(event_id=event_id))
        await self.session.commit()

        return InventoryResponse(
            event_id=event_id,
            type=InventoryResponseType.INVENTORY_RESERVED,
            order_id=event.order_id,
        )
