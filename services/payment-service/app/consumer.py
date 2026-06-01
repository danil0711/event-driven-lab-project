import json
from aiokafka import AIOKafkaConsumer

from app.core.config import KafkaTopic, get_settings


settings = get_settings()


class KafkaConsumer:
    def __init__(self, topic: KafkaTopic, group_id: str):
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            enable_auto_commit=False,
        )

    async def start(self):
        await self.consumer.start()

    async def stop(self):
        await self.consumer.stop()

    async def listen(self):
        async for msg in self.consumer:
            yield msg.value
