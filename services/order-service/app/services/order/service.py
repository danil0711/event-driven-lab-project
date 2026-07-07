import time
import uuid

from app.core.logger import logger
from app.core.tracing import get_tracer
from app.errors.order import ProductNotFoundError
from app.events.orders import OrderCreatedEvent
from app.models.orders import Order, OrderStatus
from app.models.outbox_events import OutboxEvent, OutboxStatus
from app.monitoring.consumer import OrderMetrics
from app.monitoring.order_create import OrdersCreateMetrics
from app.services.order.schema import OrderItem

_PRICES = {10: 50, 20: 100}

tracer = get_tracer()


class OrderService:
    def __init__(self, session):
        self.session = session

    async def create_order(
        self, user_id: int, items: list[OrderItem], request_id: str
    ) -> Order:
        
        with tracer.start_as_current_span("order.create"):

            log = logger.bind(request_id=request_id)

            log.info("Начало создания заказа")

            start = time.perf_counter()

            try:
                with tracer.start_as_current_span("order.calculate_total"):
                    total_amount = sum(
                        item.quantity * self.get_price(item.product_id) for item in items
                    )

                order = Order(
                    user_id=user_id,
                    status=OrderStatus.CREATED.value,
                    total_amount=total_amount,
                )

                with tracer.start_as_current_span("order.save_db"):

                    start_db = time.perf_counter()

                    self.session.add(order)
                    await self.session.flush()

                    OrdersCreateMetrics.observe_orders_create_db_latency(
                        time.perf_counter() - start_db
                    )

                log.bind(order_id=order.id).info("Заказ создан")

                event = OrderCreatedEvent(
                    event_id=str(uuid.uuid4()),
                    order_id=order.id,
                    user_id=user_id,
                    items=[i.model_dump() for i in items],
                    total_amount=total_amount,
                )

                outbox_start = time.perf_counter()


                with tracer.start_as_current_span("order.write_outbox"):
                    await self._write_outbox(event)
                    await self.session.flush()

                OrdersCreateMetrics.observe_orders_outbox_seconds(time.perf_counter() - outbox_start)

                log.bind(
                    order_id=order.id,
                    event_id=event.event_id,
                ).info("Заказ записан в Outbox")

                OrderMetrics.inc_orders_created_total()

                return order

            except ProductNotFoundError as e:
                OrderMetrics.inc_order_creation_errors_total()
                log.bind(reason=str(e)).error("Product not found")
                raise

            except Exception:
                OrderMetrics.inc_order_creation_errors_total()
                log.exception("Ошибка создания заказа")
                raise

            finally:
                OrderMetrics.observe_order_creation_seconds(time.perf_counter() - start)

    def get_price(self, product_id: int) -> int:
        price = _PRICES.get(product_id)

        if price is None:
            # вместо падения даём понятную ошибку
            raise ProductNotFoundError(product_id)

        return price

    async def _write_outbox(self, event: OrderCreatedEvent):
        self.session.add(
            OutboxEvent(
                event_id=event.event_id,
                type=event.type,
                payload=event.model_dump(mode="json"),
                status=OutboxStatus.PENDING.value,
            )
        )
