from prometheus_client import Counter, Histogram

_ORDERS_CREATED_TOTAL = Counter(
    "orders_created_total",
    "Successfully created orders",
)

_ORDER_CREATION_ERRORS_TOTAL = Counter(
    "order_creation_errors_total",
    "Order creation failures",
)

_ORDER_CREATION_SECONDS = Histogram(
    "order_creation_seconds",
    "Order creation duration in seconds",
    buckets=[
        0.001,   # 1ms
        0.002,   # 2ms
        0.003,   # 3ms
        0.004,   # 4ms
        0.005,   # 5ms
        0.01,    # 10ms
        0.025,   # 25ms
        0.05,    # 50ms
        0.1,     # 100ms
        0.25,    # 250ms
        0.5,     # 500ms
        1.0,     # 1s
        2.5,     # 2.5s
        5.0      # 5s
    ]
)


class OrderMetrics:
    @staticmethod
    def inc_orders_created_total() -> None:
        _ORDERS_CREATED_TOTAL.inc()

    @staticmethod
    def inc_order_creation_errors_total() -> None:
        _ORDER_CREATION_ERRORS_TOTAL.inc()

    @staticmethod
    def observe_order_creation_seconds(duration: float) -> None:
        _ORDER_CREATION_SECONDS.observe(duration)