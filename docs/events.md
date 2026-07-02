## Event Model

The system is built around Kafka events that represent state transitions in the saga.

Each event contains:

- `event_id` (UUID v4) → for idempotency
- `order_id` → business identifier
- `timestamp`
- `payload` → event-specific data

---

## Event Types

### Order Events

- `order_created`
- emitted by Order Service

---

### Payment Events

- `payment_success`
- `payment_failed`
- emitted by Payments Saga Worker

---

### Inventory Events

- `inventory_reserved`
- `inventory_failed`
- `inventory_skipped`  *(if payments_failed)*

emitted by Inventory Saga Worker

---

## Event Flow

order_created -> payment_succ -> inventory_reserved

or failure at any step → compensating flow (future extension)

---

## Design Rules

- Events are immutable
- Events are append-only
- No event is updated or deleted
- Consumers are responsible for idempotency