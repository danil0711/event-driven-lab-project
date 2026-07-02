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

_OUTBOX_FETCH_SECONDS = Histogram(
    "outbox_fetch_seconds", "Time spent fetching outbox evens", buckets=_BUCKETS
)


class OutboxMetrics:
    @staticmethod
    def observe_outbox_fetch_seconds(duration: float) -> None:
        _OUTBOX_FETCH_SECONDS.observe(duration)
