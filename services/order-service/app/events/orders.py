from pydantic import BaseModel

from app.services.order.schema import OrderItem


class OrderCreatedEvent(BaseModel):
    event_id: str
    type: str = "order_created"
    order_id: int
    user_id: int
    items: list[OrderItem]
    total_amount: int