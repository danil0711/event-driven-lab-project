import json
import asyncio
from aiokafka import AIOKafkaProducer

from app.core.config import settings

class KafkaProducer:
    def __init__(self):
        self.producer: AIOKafkaProducer | None = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        for attempt in range(30):
            try:
                await self.producer.start()
                print("Kafka connected")
                return
            except Exception as e:
                print(f"Kafka unavailable ({attempt+1}/30): {e}")
                await asyncio.sleep(2)

        raise RuntimeError("Could not connect to Kafka")

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def publish(self, topic: str, message: dict):
        if not self.producer:
            raise RuntimeError("Kafka producer not started")

        await self.producer.send_and_wait(topic, message)