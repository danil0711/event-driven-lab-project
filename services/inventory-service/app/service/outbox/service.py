from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.sql import or_
from sqlalchemy.orm import Session

from app.core.logger import logger
from app.models.outbox_events import OutboxEvent, OutboxStatus


BACKOFF = {
    1: 3,
    2: 10,
    3: 30,
    4: 60,
}

MAX_RETRIES = 5


def _backoff_seconds(retry_count: int) -> int:
    return BACKOFF.get(retry_count, 60)


class OutboxPublisher:
    def __init__(self, session: Session, kafka, topic: str):
        self.session = session
        self.kafka = kafka
        self.topic = topic

    async def run_once(self):
        now = datetime.now(timezone.utc)

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
            .order_by(OutboxEvent.next_attempt_at.asc())
        )

        events = result.scalars().all()

        for event in events:
            log = logger.bind(
                event_id=event.event_id,
                topic=self.topic,
            )
            log.info("Outbox worker получил event")
            try:
                await self.kafka.send(self.topic, event.payload)

                log.info(
                    "Событие отправлено в Kafka, status={}",
                    OutboxStatus.SENT.value,
                )

                event.status = OutboxStatus.SENT.value

            except Exception as e:
                log.exception("Не удалось отправить событие в Kafka")
                event.retry_count += 1
                event.last_error = str(e)

                if event.retry_count >= MAX_RETRIES:
                    event.status = OutboxStatus.FAILED.value

                    await self.kafka.send(
                        f"{self.topic}.dlq",
                        event.payload,
                    )

                    log.error(
                        "Событие помещено в DLQ, retry_count={}",
                        event.retry_count,
                    )

                else:
                    event.status = OutboxStatus.PENDING.value
                    delay = _backoff_seconds(event.retry_count)
                    event.next_attempt_at = datetime.now(timezone.utc) + timedelta(
                        seconds=delay
                    )

                    log.warning(
    "Повторная попытка отправки, retry_count={}, next_attempt_at={}",
    event.retry_count,
    event.next_attempt_at.isoformat(),
)

        await self.session.commit()
