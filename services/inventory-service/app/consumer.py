import json
from aiokafka import AIOKafkaConsumer

from app.core.config import KafkaTopic, get_settings


settings = get_settings()

def _deserialize(v):
    try:
        return json.loads(v.decode("utf-8"))
    
    except Exception as e:
        print("BAD MESSAGE:", v)
        return None


class KafkaConsumer:
    def __init__(self, topic: KafkaTopic, group_id: str):
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            value_deserializer=_deserialize,
        )

    async def start(self):
        await self.consumer.start()

    async def stop(self):
        await self.consumer.stop()

    async def listen(self):
        async for msg in self.consumer:
            yield msg.value
