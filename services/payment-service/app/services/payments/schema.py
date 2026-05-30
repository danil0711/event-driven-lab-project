from enum import Enum
from pydantic import BaseModel


class PaymentType(str, Enum):
    SUCCESS = "payment_success"
    FAILED = "payment_failed"


class OrderItem(BaseModel):
    product_id: int
    quantity: int


class OrderCreatedIntegrationEvent(BaseModel):
    event_id: str
    type: str = "order_created"
    order_id: int
    user_id: int
    items: list[OrderItem]
    total_amount: int


class PaymentProcessEvent(BaseModel):
    event_id: str
    order_id: int
    type: PaymentType
    reason: str | None = None
    items: list[OrderItem]
