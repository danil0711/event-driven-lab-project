from typing import Sequence
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select

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
    def __init__(self, session, kafka, topic):
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
            .order_by(OutboxEvent.next_attempt_at.asc())
            .limit(100)
        )

        events: Sequence[OutboxEvent] = result.scalars().all()

        for event in events:
            log = logger.bind(event_id=event.event_id)
            log.info("Outbox воркер получил event.")
            try:
                await self.kafka.send(self.topic, event.payload)
                log.info(
                    "Outbox отправил событие, topic={}, status={}",
                    self.topic,
                    OutboxStatus.SENT.value,
                )

                event.status = OutboxStatus.SENT.value

            except Exception as e:
                log.exception("Outbox не смог отправить сообщение")

                event.retry_count += 1
                event.last_error = str(e)

                if event.retry_count >= MAX_RETRIES:
                    event.status = OutboxStatus.FAILED.value

                    await self.kafka.send(
                        f"{self.topic}.dlq",
                        event.payload,
                    )

                    log.error(
                        "Outbox поместил event в DLQ",
                        retry_count=event.retry_count,
                    )

                else:
                    event.status = OutboxStatus.PENDING.value

                    delay = _backoff_seconds(event.retry_count)
                    event.next_attempt_at = datetime.now(timezone.utc) + timedelta(
                        seconds=delay
                    )

                    log.warning(
                        "Outbox ретраит event",
                        retry_count=event.retry_count,
                        next_attempt_at=event.next_attempt_at.isoformat(),
                    )

        await self.session.commit()
