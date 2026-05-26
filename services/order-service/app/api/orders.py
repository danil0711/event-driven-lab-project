from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db import get_session
from app.dependencies.kafka import get_kafka
from app.services.order.service import OrderService
from app.services.order.schema import CreateOrderRequest

router = APIRouter(prefix="/orders", tags=["orders"])


# временно глобальный producer (потом можно улучшить DI)

settings = get_settings()

def get_service(
    session: Session = Depends(get_session),
    kafka=Depends(get_kafka),
):
    return OrderService(
        session=session,
        kafka=kafka,
        topic=settings.kafka_orders_topic,
    )


@router.post("")
async def create_order(
    request: CreateOrderRequest,
    service: OrderService = Depends(get_service),
):
    try:
        return await service.create_order(
            user_id=request.user_id,
            items=request.items,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))