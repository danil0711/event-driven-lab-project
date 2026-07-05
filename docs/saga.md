## Saga Flow Summary

The system implements a **hybrid event-driven saga model** where the **Order Service is the single source of truth for saga state**.

---

### 1. Order creation
- Client creates order in **Order Service**
- Order is persisted in Postgres
- Outbox Worker publishes `order-events` to Kafka

---

### 2. Payment processing (Payment Service)
- Consumes `order-events`
- Executes payment logic
- Emits:
  - `payment_succeeded`
  - `payment_failed`

- Writes result to Payment Outbox → Kafka

---

### 3. Payment Saga Handler (Order service)
- Consumes `payment-events`
- Updates Order state:
  - `PAID` if success
  - `FAILED` if payment failed

---

### 4. Inventory processing (Inventory Service)
- Consumes **only `payment_succeeded`**
- Executes stock reservation
- Emits `inventory-events`:
  - `inventory_reserved`
  - `inventory_failed`

- Inventory does NOT participate in order decision logic

---

### 5. Inventory Saga Handler (Order events)
- Consumes `inventory-events`
- Updates Order state:
  - `COMPLETED` if reserved
  - `CANCELLED` if failed

---

### 6. Refund flow (Payment compensation)

- Inventory Service emits `inventory_failed`
- Payment Saga Handler consumes `inventory_failed`
- Payment Service creates `REFUND` event
- Refund is emitted via Payment Outbox → Kafka (`payment-events`)

- Order Saga Handler consumes:
  - `payment_refunded`
- Updates Order state:
  - `CANCELLED`

---

### 7. Final state ownership
- **Order Service is the only system that owns saga state**
- All other services are side-effect executors
- They never decide final order status