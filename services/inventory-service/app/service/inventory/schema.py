from pydantic import BaseModel
from enum import Enum


class PaymentType(str, Enum):
    SUCCESS = "payment_success"
    FAILED = "payment_failed"


class OrderItem(BaseModel):
    product_id: int
    quantity: int


class PaymentEvent(BaseModel):
    order_id: int
    type: PaymentType
    items: list[OrderItem]
    reason: str | None = None

class InventoryResponse(BaseModel):
    type: str
    order_id: int
    reason: str | None = None
    product_id: int | None = None