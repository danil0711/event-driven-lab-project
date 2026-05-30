import uuid

from sqlalchemy.exc import SQLAlchemyError

from app.events.orders import OrderCreatedEvent
from app.models.orders import Order, OrderStatus
from app.models.outbox_events import OutboxEvent, OutboxStatus
from app.services.order.schema import OrderItem

_PRICES = {10: 50, 20: 100}


class OrderService:
    def __init__(self, session):
        self.session = session

    async def create_order(self, user_id: int, items: list[OrderItem]):
        total_amount = sum(
            item.quantity * self.get_price(item.product_id) for item in items
        )

        order = Order(
            user_id=user_id, status=OrderStatus.CREATED.value, total_amount=total_amount
        )

        self.session.add(order)
        await self.session.flush()

        event = OrderCreatedEvent(
            event_id=str(uuid.uuid4()),
            order_id=order.id,
            user_id=user_id,
            items=[i.model_dump() for i in items],
            total_amount=total_amount,
        )

        outbox_event = OutboxEvent(
            event_id=event.event_id,
            type=event.type,
            payload=event.model_dump(mode="json"),
            status=OutboxStatus.PENDING.value,
        )

        self.session.add(outbox_event)

        try:
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            raise

        await self.session.refresh(order)

        return order

    def get_price(self, product_id: int) -> int:
        price = _PRICES.get(product_id)

        if price is None:
            # вместо падения даём понятную ошибку
            raise ValueError(f"Продукт не найден по айди: {product_id}")

        return price
