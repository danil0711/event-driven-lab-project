from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.infrastructure.kafka.producer import KafkaProducer
from app.api.orders import router as order_router

app = FastAPI()





@asynccontextmanager
async def lifespan(app: FastAPI):
    kafka = KafkaProducer()
    await kafka.start()

    # кладём в app.state (ВАЖНО)
    app.state.kafka = kafka

    yield

    await kafka.stop()

app = FastAPI(lifespan=lifespan)

app.include_router(order_router)


@app.get("/health")
async def healthcheck() -> dict:
    return {"status": "ok"}
