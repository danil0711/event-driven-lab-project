## Retry Strategy and DLQ (Dead Letter Queue)

All event-driven components in the system implement a **retry + backoff + DLQ mechanism** to ensure reliability under failures.

---

## Retry Policy

Each event is processed with exponential backoff:

Retry delays:

- 1st retry → 3s
- 2nd retry → 10s
- 3rd retry → 30s
- 4th retry → 60s
- 5th+ retry → 60s (capped)

Maximum retries: **5 attempts**

---

## Failure Handling Flow

When a processing error occurs:

1. Event retry counter is incremented
2. Error is stored (`last_error`)
3. Next retry is scheduled using backoff strategy
4. Event remains in `PENDING` state

---

## Dead Letter Queue (DLQ)

If event fails after max retries:

- Event status is marked as `FAILED`
- Event is published to Kafka DLQ topic:
  - `<topic>.dlq`

This ensures:

- no silent message loss
- ability to inspect failed events
- optional manual reprocessing

---

## Design Goals

This mechanism ensures:

- at-least-once delivery safety
- resilience to temporary Kafka/Postgres failures
- controlled retry pressure (no hot looping)
- observability of failed events

---

## Backoff Implementation

Backoff is deterministic and stored per event:

- each retry increases delay
- delay is calculated as:

retry_count → delay seconds

This prevents:
- retry storms
- Kafka overload
- database pressure spikes