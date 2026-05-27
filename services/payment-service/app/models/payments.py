from sqlalchemy import Column, Integer, String

from app.models.base import Base


class Payment(Base):
    __tablename__ = "payments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, unique=True)
    status = Column(String)  # SUCCESS / FAILED
    amount = Column(Integer)
