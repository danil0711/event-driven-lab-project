from aiokafka import AIOKafkaProducer
import json


class KafkaProducer:
    def __init__(self, settings):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    async def start(self):
        await self.producer.start()

    async def stop(self):
        await self.producer.stop()

    async def send(self, topic: str, message: dict):
        await self.producer.send_and_wait(topic, message)