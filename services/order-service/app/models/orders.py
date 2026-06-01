import enum

from sqlalchemy import Column, Integer, String

from app.models.base import Base


class OrderStatus(str, enum.Enum):
    CREATED = "created"

    PAID = "paid"
    FAILED = "failed"

    CANCELLED = "cancelled"

    COMPLETED = "completed"


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    status = Column(String, nullable=False, default=OrderStatus.CREATED.value)
    total_amount = Column(Integer, default=0)
