from prometheus_client import Histogram


_BUCKETS = [
    0.001,  # 1ms
    0.002,  # 2ms
    0.003,  # 3ms
    0.004,  # 4ms
    0.005,  # 5ms
    0.01,  # 10ms
    0.025,  # 25ms
    0.05,  # 50ms
    0.1,  # 100ms
    0.25,  # 250ms
    0.5,  # 500ms
    1.0,  # 1s
    2.5,  # 2.5s
    5.0,  # 5s
]

_ORDERS_DB_SECONDS = Histogram(
    "order_create_db_seconds", "Time spent writing order to database", buckets=_BUCKETS
)

_ORDERS_OUTBOX_SECONDS = Histogram(
    "order_create_outbox_seconds",
    "Time spent writing order event to outbox",
    buckets=_BUCKETS,
)

_ORDERS_PUBLISH_SECONDS = Histogram(
    "orders_publish_seconds", "Orders kafka publish duration", buckets=_BUCKETS
)


class OrdersCreateMetrics:
    @staticmethod
    def observe_orders_create_db_latency(duration: float) -> None:
        _ORDERS_DB_SECONDS.observe(duration)

    @staticmethod
    def observe_orders_outbox_seconds(duration: float) -> None:
        _ORDERS_OUTBOX_SECONDS.observe(duration)

    @staticmethod
    def observe_kafka_publish_seconds(duration: float) -> None:
        _ORDERS_PUBLISH_SECONDS.observe(duration)
