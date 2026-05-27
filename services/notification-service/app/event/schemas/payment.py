from enum import Enum
from pydantic import BaseModel

from app.event.schemas.common import OrderItem


class PaymentType(str, Enum):
    SUCCESS = "payment_success"
    FAILED = "payment_failed"


class PaymentProcessEvent(BaseModel):
    order_id: int
    type: PaymentType
    reason: str | None = None
    items: list[OrderItem]
