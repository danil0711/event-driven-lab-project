from enum import Enum

from pydantic import BaseModel


class InventoryResponseType(str, Enum):
    INVENTORY_SKIPPED = "inventory_skipped"
    INVENTORY_FAILED = "inventory_failed"
    INVENTORY_RESERVED = "inventory_reserved"


class InventoryResponse(BaseModel):
    type: InventoryResponseType
    order_id: int
    reason: str | None = None
    product_id: int | None = None
