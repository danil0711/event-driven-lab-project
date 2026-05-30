from sqlalchemy import Column, String

from app.models.base import Base

# TODO удалить
class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id = Column(String, primary_key=True)