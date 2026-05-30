# Остатки товаров
import random

from app.core.logger import logger
from app.models.outbox_events import OutboxEvent
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

    async def process(self, event: PaymentEvent) -> None:
        # if random.random() < 0.5:
        #     raise Exception("Inventory service crashed")

        response = await self._handle(event)

        await self._write_outbox(response)


    async def _handle(self, event: PaymentEvent) -> InventoryResponse:
        event_id = event.event_id

        if event.type != PaymentType.SUCCESS:
            logger.debug('Payment не success')
            return InventoryResponse(
                event_id=event_id,
                type=InventoryResponseType.INVENTORY_SKIPPED,
                order_id=event.order_id,
            )

        for item in event.items:
            available = STOCK.get(item.product_id)

            if available < item.quantity:
                logger.debug('Inventory failed')
                return InventoryResponse(
                    event_id=event_id,
                    type=InventoryResponseType.INVENTORY_FAILED,
                    order_id=event.order_id,
                    reason="out_of_stock",
                    product_id=item.product_id,
                )

        for item in event.items:
            STOCK[item.product_id] -= item.quantity

        return InventoryResponse(
            event_id=event_id,
            type=InventoryResponseType.INVENTORY_RESERVED,
            order_id=event.order_id,
        )

    async def _write_outbox(self, response: InventoryResponse):
        self.session.add(
            OutboxEvent(
                event_id=response.event_id,
                type=response.type,
                payload=response.model_dump(mode="json"),
            )
        )
