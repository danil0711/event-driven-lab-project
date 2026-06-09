import asyncio

from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from app.core.logger import logger
from app.core.config import get_settings

from aiokafka.errors import KafkaConnectionError

from app.producer import KafkaProducer

settings = get_settings()


async def ensure_topics() -> None:
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )

    await admin.start()

    try:
        existing_topics = await admin.list_topics()

        required_topics = [
            settings.kafka_payments_topic,
            settings.kafka_inventory_topic,
        ]

        missing_topics = [
            NewTopic(
                name=topic,
                num_partitions=3,
                replication_factor=1,
            )
            for topic in required_topics
            if topic not in existing_topics
        ]

        if missing_topics:
            logger.info("Создаю кафка-топики.")
            await admin.create_topics(missing_topics)
        else:
            logger.info('Все кафка топики уже существуют.')

    finally:
        await admin.close()


async def start_kafka_with_retry(
    kafka: KafkaProducer,
    attempts: int = 30,
    delay: int = 2,
) -> None:
    for attempt in range(1, attempts + 1):
        try:
            await kafka.start()

            logger.info(
                "Kafka connected on attempt %s",
                attempt,
            )
            return

        except KafkaConnectionError:
            logger.warning(
                "Kafka unavailable. Attempt %s/%s",
                attempt,
                attempts,
            )

        await asyncio.sleep(delay)

    raise RuntimeError("Kafka startup timeout")