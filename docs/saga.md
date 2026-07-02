## Saga Flow Summary

The system implements a **hybrid event-driven saga model**:

1. Order is created in Order Service
2. Outbox Worker publishes `order-events` to Kafka

3. Payment Service processes the order
   - consumes `order-events`
   - executes payment logic
   - emits `payment-events` (`payment_succeeded` / `payment_failed`)

4. Payment Saga Handler (Order-side consumer)
   - consumes `payment-events`
   - updates Order state in Postgres

5. Inventory Service processes successful payments
   - consumes `payment-events`
   - executes inventory reservation
   - emits `inventory-events`

6. Inventory Saga Handler (Order-side consumer)
   - consumes `inventory-events`
   - updates Order state in Postgres

7. Order Service becomes the **system of record for saga state**
   - final order status is derived from event progression

