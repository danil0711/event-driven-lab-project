from sqlalchemy import Column, String

from app.models.base import Base


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id = Column(String, primary_key=True)
