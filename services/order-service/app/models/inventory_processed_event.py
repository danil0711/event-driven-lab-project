from sqlalchemy import Column, String

from app.models.base import Base


class InventoryProcessedEvent(Base):
    __tablename__ = "inventory_processed_events"

    event_id = Column(String, primary_key=True)