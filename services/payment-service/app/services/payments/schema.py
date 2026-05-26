import random

from app.services.payments.service import PaymentProcessEvent


class PaymentService:
    def process(self, event: dict) -> dict:
        # симуляция оплаты
        if random.random() < 0.7:
            return PaymentProcessEvent(
                type="payment_success", order_id=event["order_id"], reason=None
            )

        else:
            return PaymentProcessEvent(
                type="payment_failed", order_id=event["order_id"], reason="random_fail"
            )
