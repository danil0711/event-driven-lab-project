
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.sql import or_
from sqlalchemy.orm import Session

from app.core.logger import logger
from app.models.outbox_events import OutboxEvent, OutboxStatus



MAX_RETRIES = 5


def _backoff_seconds(retry_count: int) -> int:
    # экспоненциальный backoff с капом
    return min(60, 2 ** retry_count)


class OutboxPublisher:
    def __init__(self, session: Session, kafka, topic: str):
        self.session = session
        self.kafka = kafka
        self.topic = topic

    async def run_once(self):
        now = datetime.utcnow()

        result = await self.session.execute(
            select(OutboxEvent)
            .where(
                OutboxEvent.status == OutboxStatus.PENDING.value,
                or_(
                    OutboxEvent.next_attempt_at.is_(None),
                    OutboxEvent.next_attempt_at <= now,
                ),
            )
            .limit(100)
        )

        events = result.scalars().all()

        for event in events:
            logger.info(f"Outbox worker got event: {event.event_id}")
            try:
                await self.kafka.send(self.topic, event.payload)

                event.status = OutboxStatus.SENT.value

            except Exception as e:
                event.retry_count += 1
                event.last_error = str(e)

                if event.retry_count >= MAX_RETRIES:
                    event.status = OutboxStatus.FAILED.value

                    # optional: можно сразу слать в DLQ Kafka topic
                    await self.kafka.send(
                        f"{self.topic}.dlq",
                        event.payload,
                    )

                else:
                    event.status = OutboxStatus.PENDING.value
                    delay = _backoff_seconds(event.retry_count)
                    event.next_attempt_at = datetime.utcnow() + timedelta(seconds=delay)

        await self.session.commit()