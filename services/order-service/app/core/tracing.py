from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter,
)

from app.core.config import get_settings

settings = get_settings()


def init_tracing() -> None:
    resource = Resource.create(
        {
            "service.name": "order-service",
        }
    )

    provider = TracerProvider(resource=resource)

    exporter = OTLPSpanExporter(
        endpoint=settings.otel_endpont,
        insecure=True,
    )

    processor = BatchSpanProcessor(exporter)

    provider.add_span_processor(processor)

    trace.set_tracer_provider(provider)


def get_tracer():
    return trace.get_tracer(__name__)
