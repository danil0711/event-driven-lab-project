from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_client import make_asgi_app

from app.bootstrap.kafka import ensure_topics, start_kafka_with_retry
from app.infrastructure.kafka.producer import KafkaProducer
from app.api.orders import router as order_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    kafka = KafkaProducer()
    await start_kafka_with_retry(kafka)
    await ensure_topics()
    
    await kafka.start()

    app.mount("/metrics", make_asgi_app())

    # кладём в app.state (ВАЖНО)
    app.state.kafka = kafka

    yield

    await kafka.stop()


app = FastAPI(lifespan=lifespan)

app.include_router(order_router)


@app.get("/health")
async def healthcheck() -> dict:
    return {"status": "ok"}
