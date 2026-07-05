from aiokafka.admin import AIOKafkaAdminClient, NewTopic
import asyncio
from app.core.logger import logger
from app.core.config import get_settings

from aiokafka.errors import KafkaConnectionError

from app.infrastructure.kafka.producer import KafkaProducer

settings = get_settings()


async def ensure_topics() -> None:
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )

    await admin.start()

    try:
        existing_topics = None

        # --- RETRY: ждём пока Kafka реально поднимется ---
        for attempt in range(1, 31):
            try:
                existing_topics = await admin.list_topics()
                break
            except Exception as e:
                logger.warning(f"Kafka not ready ({attempt}/30): {e}")
                await asyncio.sleep(min(2 * attempt, 10))  # backoff

        if existing_topics is None:
            raise RuntimeError("Kafka did not become ready in time")

        required_topics = [settings.kafka_orders_topic]

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
            logger.info("Все кафка топики уже существуют.")

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
                "Kafka connected on attempt {}",
                attempt,
            )
            return

        except KafkaConnectionError:
            logger.warning(
                "Kafka unavailable. Attempt {}/{}",
                attempt,
                attempts,
            )

        await asyncio.sleep(delay)

    raise RuntimeError("Kafka startup timeout")