from app.core.logger import logger
from app.event.schemas.inventory import InventoryResponse, InventoryResponseType



async def handle_inventory_event(event: InventoryResponse):
    if event.type == InventoryResponseType.INVENTORY_FAILED:
        logger.warning(f"Товар {event.product_id} закончился (order {event.order_id})")

    elif event.type == InventoryResponseType.INVENTORY_RESERVED:
        logger.info(f"Зарезервирован заказ {event.order_id}")

    elif event.type == InventoryResponseType.INVENTORY_SKIPPED:
        logger.debug(f"Пропущен заказ {event.order_id}")