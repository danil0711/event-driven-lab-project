from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_client import make_asgi_app

from app.bootstrap.kafka import ensure_topics
from app.infrastructure.kafka.producer import KafkaProducer
from app.api.orders import router as order_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ensure_topics()
    kafka = KafkaProducer()
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
