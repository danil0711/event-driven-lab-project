import json
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
        await self.producer.start()

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def publish(self, topic: str, message: dict):
        if not self.producer:
            raise RuntimeError("Kafka producer not started")

        await self.producer.send_and_wait(topic, message)