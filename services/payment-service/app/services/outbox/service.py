from sqlalchemy import select

from app.models.outbox_events import OutboxEvent, OutboxStatus


class OutboxPublisher:
    def __init__(self, session, kafka, topic):
        self.session = session
        self.kafka = kafka
        self.topic = topic

    async def run_once(self):

        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.PENDING.value)
            .with_for_update(skip_locked=True)
            .limit(100)
        )

        async with self.session.begin():
            result = await self.session.execute(stmt)

            events = result.scalars().all()

            for event in events:
                print(f"Обработка event: {event.event_id}")
                try:
                    await self.kafka.send(self.topic, event.payload)
                    event.status = OutboxStatus.SENT.value

                    print(f"Отправлено outbox событие {event.event_id}.")

                except Exception:
                    print(f"outbox событие {event.event_id} не было отправлено.")
                    event.status = OutboxStatus.FAILED.value
