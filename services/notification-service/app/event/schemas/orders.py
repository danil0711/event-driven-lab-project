from pydantic import BaseModel

from app.event.schemas.common import OrderItem


class OrderCreatedEvent(BaseModel):
    event_id: str
    type: str = "order_created"
    order_id: int
    user_id: int
    items: list[OrderItem]
    total_amount: int
