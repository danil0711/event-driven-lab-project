
from pydantic import BaseModel

class OrderItem(BaseModel):
    product_id: int
    quantity: int


class CreateOrderRequest(BaseModel):
    user_id: int
    items: list[OrderItem]