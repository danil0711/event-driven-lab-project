import enum

from sqlalchemy import JSON, Column, Integer, String
from app.models.base import Base

class OutboxStatus(str, enum.Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id = Column(Integer, primary_key=True, autoincrement=True)

    event_id = Column(String, nullable=False, unique=True)
    type = Column(String, nullable=False)

    payload = Column(JSON, nullable=False)

    status = Column(String, nullable=False, default=OutboxStatus.PENDING.value)
