# Consistent Store

This project is a learning lab for practicing and demonstrating understanding of:

- hexagonal architecture
- domain-oriented package boundaries in Go
- transactional outbox
- Kafka
- Debezium CDC
- distributed tracing with OpenTelemetry and Jaeger

The application is intentionally small, but it includes enough moving parts to show how consistency, event publishing, consumers, and trace correlation work in a realistic service flow.

## What It Demonstrates

The main business flow is:

```text
HTTP request
  -> order/customer domain service
  -> PostgreSQL transaction
  -> outbox_events insert
  -> Debezium reads PostgreSQL WAL
  -> Kafka event
  -> analytics and notification consumers
```

The project uses the transactional outbox pattern so domain changes and event creation are committed atomically in the same database transaction.

## Architecture

The newer code is organized under:

```text
internal/domains
```

Each domain follows a hexagonal architecture style:

```text
entities
services
ports/in
ports/out
adapters/in
adapters/out
```

Examples:

- `order` exposes HTTP input and writes orders/outbox events through output adapters.
- `inventory` exposes reservation/query use cases and a PostgreSQL output adapter.
- `analytics` consumes Kafka events and updates a read model.
- `notification` consumes Kafka events and writes/logs notification side effects.

Shared infrastructure lives under:

```text
internal/shared
```

## Services

Docker Compose starts:

- PostgreSQL
- three Kafka brokers in KRaft mode
- Kafka Connect with Debezium
- Kafka UI
- order-service
- analytics-consumer
- notification-service
- OpenTelemetry Collector
- Jaeger

Useful local URLs:

```text
Order service: http://localhost:8080
Kafka UI:      http://localhost:8081
Kafka Connect: http://localhost:8083
Jaeger UI:     http://localhost:16686
```

## Documentation

Start here:

- [RUNBOOK.md](RUNBOOK.md): commands to start the stack and verify the demo flow.
- [OUTBOX_TO_KAFKA.md](OUTBOX_TO_KAFKA.md): how an `outbox_events` row becomes a Kafka message through Debezium.
- [TRACING.md](TRACING.md): how OpenTelemetry trace context crosses HTTP, outbox rows, Debezium, Kafka, and consumers.

## Quick Start

Start everything:

```bash
docker compose up -d --build
```

For a clean reset:

```bash
docker compose down -v --remove-orphans
docker compose up -d --build
```

Create a customer:

```bash
curl -sS -X POST http://localhost:8080/customers \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"customer-001"}'
```

Create an order:

```bash
curl -sS -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"customer-001","items":[{"sku":"BOOK-001","quantity":1}]}'
```

Then inspect:

- Kafka events in Kafka UI
- read model rows in PostgreSQL
- traces in Jaeger
- service logs through Docker Compose

See [RUNBOOK.md](RUNBOOK.md) for the full verification flow.

## Main Learning Points

This repository is not trying to be a production template. It is meant to make the mechanics visible:

- how ports decouple domain services from infrastructure
- how output adapters hide database and outbox details
- how Debezium routes outbox rows to Kafka topics
- how Kafka consumers filter by event type
- how idempotent consumers avoid duplicate processing
- how `traceparent` and `tracestate` are propagated through an asynchronous event pipeline

## Current Event Flow

Customer creation:

```text
POST /customers
  -> CustomerCreated outbox event
  -> customer.events
  -> notification-service logs customer notification
```

Order creation:

```text
POST /orders
  -> inventory reserved
  -> order stored
  -> OrderCreated outbox event
  -> order.events
  -> analytics-consumer updates order analytics
  -> notification-service records notification
  -> OrderNotificationSent outbox event
```
