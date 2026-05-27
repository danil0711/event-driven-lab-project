from pydantic import BaseModel
from enum import Enum


class PaymentType(str, Enum):
    SUCCESS = "payment_success"
    FAILED = "payment_failed"


class InventoryResponseType(str, Enum):
    INVENTORY_SKIPPED = "inventory_skipped"
    INVENTORY_FAILED = "inventory_failed"
    INVENTORY_RESERVED = "inventory_reserved"


class OrderItem(BaseModel):
    product_id: int
    quantity: int


class PaymentEvent(BaseModel):
    order_id: int
    type: PaymentType
    items: list[OrderItem]
    reason: str | None = None


class InventoryResponse(BaseModel):
    type: InventoryResponseType
    order_id: int
    reason: str | None = None
    product_id: int | None = None
