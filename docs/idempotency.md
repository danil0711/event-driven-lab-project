# Idempotency Strategy

All consumers in the system are designed to handle duplicate Kafka events.

Since Kafka provides **at-least-once delivery**, duplicate messages are expected.

---

## Solution: processed_events table

Each service uses a `processed_events` table to guarantee idempotent processing.

### Schema

- `event_id (UUID v4)` — unique event identifier
- `processed_at` — timestamp of processing

---

## Processing Flow

Before processing any event:

1. Extract `event_id` from Kafka message
2. Check if event exists in `processed_events`
3. If exists → skip processing
4. If not:
   - process event
   - insert `event_id` into `processed_events`

---

## Why UUID v4?

- globally unique across services
- safe for distributed systems
- no coordination required

---

## Guarantees

This approach ensures:

- no duplicate side effects (payments, inventory reservations)
- safe retries on worker crash
- safe Kafka re-delivery
- eventual consistency correctness



# Transaction Commit Semantics

All services follow a strict rule:

> Database commit happens only after successful event processing.

---

## Why this matters

Since the system is event-driven with at-least-once delivery:

- Kafka events may be duplicated
- Consumers may retry after failures
- Workers may crash mid-processing

To ensure consistency, we enforce:

> "No commit before successful event handling"

---

## Processing Rule

Each consumer follows this pattern:

1. Receive event from Kafka
2. Check idempotency (processed_events)
3. Execute business logic
4. Persist state changes
5. Write outbox event (if applicable)
6. Commit database transaction

---

## Guarantee

This ensures:

- no partial state commits
- safe retries
- idempotent processing
- consistent saga progression

---

## Important Note

Commit is not just a technical DB operation — it represents:

> successful completion of an event handling step