import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.tracing import get_tracer
from app.db import get_session
from app.errors.order import OrderError, ProductNotFoundError
from app.services.order.service import OrderService
from app.services.order.schema import CreateOrderRequest

router = APIRouter(prefix="/orders", tags=["orders"])

settings = get_settings()

tracer = get_tracer()


@router.post("")
async def create_order(
    request: CreateOrderRequest,
    session: AsyncSession = Depends(get_session),
):
    
    with tracer.start_as_current_span("http.create_order"):
    
        service = OrderService(session)
        request_id = uuid.uuid4()

        try:
            async with session.begin():
                order = await service.create_order(
                    user_id=request.user_id,
                    items=request.items,
                    request_id=request_id
                    
                )

            return {
                "order_id": order.id,
                "status": order.status,
            }


        except ProductNotFoundError as e:
            raise HTTPException(status_code=400, detail=str(e))

        except OrderError as e:
            raise HTTPException(status_code=422, detail=str(e))

        except SQLAlchemyError:
            raise HTTPException(status_code=500, detail="Database error")

        except Exception:
            raise HTTPException(status_code=500, detail="Internal server error")