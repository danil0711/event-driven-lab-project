from enum import Enum
from pydantic import BaseModel


class PaymentType(str, Enum):
    SUCCESS = "payment_success"
    FAILED = "payment_failed"


class PaymentProcessEvent(BaseModel):
    order_id: int
    type: PaymentType
    reason: str | None = None
