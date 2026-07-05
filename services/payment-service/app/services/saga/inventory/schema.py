from enum import StrEnum

from pydantic import BaseModel


class InventoryResponseType(StrEnum):
    INVENTORY_FAILED = "inventory_failed"
    INVENTORY_RESERVED = "inventory_reserved"


class InventoryResponse(BaseModel):
    event_id: str
    type: InventoryResponseType
    order_id: int
    reason: str | None = None
    product_id: int | None = None
