# Остатки товаров
import random

from app.service.inventory.schema import InventoryResponse, PaymentEvent, PaymentType


STOCK = {10: 50, 20: 100}


class InventoryService:
    def proccess(self, event: PaymentEvent) -> InventoryResponse:
        if random.random() < 0.1:
            raise Exception("Inventory service crashed")
        
        

        if event.type != PaymentType.SUCCESS:
            return InventoryResponse(type="inventory_skipped", order_id=event.order_id)

        for item in event.items:
            available = STOCK.get(item.product_id)

            if available < item.quantity:
                return InventoryResponse(
                    type="inventory_failed",
                    order_id=event.order_id,
                    reason="out_of_stock",
                    product_id=item.product_id,
                )

        for item in event.items:
            STOCK[item.product_id] -= item.quantity

        return InventoryResponse(type="inventory_reserved", order_id=event.order_id)
