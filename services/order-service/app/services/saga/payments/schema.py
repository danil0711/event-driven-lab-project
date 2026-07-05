from enum import Enum

from pydantic import BaseModel


class PaymentType(str, Enum):
    SUCCESS = "payment_success"
    FAILED = "payment_failed"
    REFUNDED = "payment_refunded"


class OrderItem(BaseModel):
    product_id: int
    quantity: int


class PaymentProcessEvent(BaseModel):
    event_id: str
    order_id: int
    type: PaymentType
    reason: str | None = None
    items: list[OrderItem]
