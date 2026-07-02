![Python](https://img.shields.io/badge/python-3.11-blue)
![Kafka](https://img.shields.io/badge/kafka-event--driven-black)
![Postgres](https://img.shields.io/badge/postgres-multi--db-blue)
![Architecture](https://img.shields.io/badge/saga-choreography-orange)
![Observability](https://img.shields.io/badge/grafana-loki-red)
![Observability](https://img.shields.io/badge/Go-00ADD8?logo=Go&logoColor=white&style=for-the-badge-)

# Kafka + Outbox + Saga System

## Overview

Event-driven microservices system demonstrating:
- Outbox pattern
- Kafka-based communication
- Choreography Saga
- Independent services
- Idempotency & retries strategy

Flow:
Order → Payment → Inventory

## Documentation

- Architecture → [docs/architecture.md](./docs/architecture.md)
- Events → [docs/events](./docs/events.md)
- Idempotency → [docs/idempotency](./docs/idempotency.md)
- Retry strategy → [docs/retry_strategy](./docs/retry_strategy.md)
- Saga Flow → [docs/saga.md](./docs/saga.md)
- Observability → [docs/observability.md](./docs/observability.md)

## How to run

This project is fully containerized and can be started using a single entrypoint script.

The whole system (infrastructure + services) is launched via:

```bash
python run.py
```

---

### What `run.py` does

The script is responsible for bootstrapping the full environment:

- Starts infrastructure:
  - PostgreSQL
  - Kafka
  - Monitoring stack (Grafana/Loki if enabled)

- Runs database migrations (to be done)

- Starts all services:
  - Order Service
  - Payment Service
  - Inventory Service
  - Outbox Workers (Order / Payment / Inventory)
  - Saga Workers
  - Notifications service (observer)

---

### Requirements

Make sure you have:

- Docker installed and running
- Python 3.11+
- Docker Compose (if used inside run.py)

---

### Recommended run flow

```bash
# 1. Clone repository
git clone <repo-url>

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start full system
python run.py
```

---

### Notes

- The system is fully event-driven (Kafka-based communication)
- Services are independent and can start in any order
- Eventual consistency is expected by design
- Logs are structured in production mode and suitable for Grafana/Loki

---

### Development mode

For local development:

- Services can be started individually
- Kafka + Postgres must be running beforehand
- Useful for debugging specific components (e.g. Saga Workers or Outbox)

---

### Important

`run.py` is the **single entry point** for running the entire distributed system locally, including infrastructure and all services.

## Technologies

### Backend
- Python 3.11
- Go (experimental service)

### Messaging
- Kafka (event-driven communication)
- Outbox Pattern (reliable event delivery)

### Databases
- PostgreSQL

### Architecture Patterns
- Event-driven architecture
- Choreography-based Saga
- Idempotency handling
- Retry + Backoff strategy
- Dead Letter Queue (DLQ)

### Observability
- Structured logging (JSON in production)
- Grafana (dashboards for latency & system metrics)
- Loki (log aggregation)

### Infrastructure
- Docker / Docker Compose
- Local orchestration via `run.py`

### Monitoring
- Custom metrics for:
  - latency (order creation, outbox, kafka publish)
  - event processing time