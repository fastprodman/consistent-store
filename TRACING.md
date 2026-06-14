# Distributed Tracing

This project uses OpenTelemetry to trace work across HTTP, PostgreSQL outbox rows, Debezium, Kafka, and Kafka consumers.

The important part is that the trace does not go directly from `order-service` to the consumers. It crosses an outbox table first.

## Local Trace Backend

`docker-compose.yaml` starts:

```text
otel-collector
jaeger
```

Application services export OTLP traces to:

```text
otel-collector:4317
```

Jaeger UI is available at:

```text
http://localhost:16686
```

Service names:

```text
order-service
analytics-consumer
notification-service
```

Tracing is initialized in each `cmd/*/main.go` through:

```go
observability.InitTracing(ctx, "<service-name>")
```

The shared tracing setup is in:

```text
internal/shared/observability/tracing.go
```

It configures:

```text
OTLP gRPC exporter
service.name resource attribute
W3C trace context propagation
```

## Trace Context

The project uses standard W3C trace context headers:

```text
traceparent
tracestate
```

`traceparent` is the most important one. It contains:

```text
version-trace_id-parent_span_id-trace_flags
```

Example:

```text
00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
```

The `trace_id` is what lets Jaeger group spans from different services into one trace.

The helper code for extracting and injecting this context is in:

```text
internal/shared/observability/propagation.go
```

## HTTP Entry Point

`order-service` has HTTP tracing middleware on these routes:

```text
POST /customers
POST /orders
GET /orders/{id}
```

The middleware creates server spans such as:

```text
POST /orders
POST /customers
```

If a request already has a `traceparent` header, the server span continues that incoming trace. If not, OpenTelemetry starts a new trace.

## In Process Spans

Inside `order-service`, application work creates child spans:

```text
order.create
inventory.reserve
outbox.publish OrderCreated
outbox.publish CustomerCreated
```

Inside consumers:

```text
analytics.consume OrderCreated
analytics.project_order_statistics
notification.consume OrderCreated
notification.consume CustomerCreated
notification.notify_order_created
outbox.publish OrderNotificationSent
```

These spans are correlated through the `context.Context` passed through service, adapter, and repository calls.

## Outbox Boundary

The outbox table has trace context columns:

```sql
traceparent TEXT,
tracestate TEXT
```

When an outbox publisher writes an event, it first starts a producer span:

```text
outbox.publish OrderCreated
```

Then it extracts trace context from the current `context.Context`:

```go
traceHeaders := observability.TraceHeadersFromContext(ctx)
```

Those values are inserted into `outbox_events`:

```text
traceparent = current span context
tracestate  = current trace state
```

This is the key bridge. Without these columns, the trace would stop at the service that wrote the outbox row.

## Debezium to Kafka Headers

Debezium reads `outbox_events` and uses the outbox event router transform.

The connector config is in:

```text
debezium/postgres-outbox-connector.json
```

This setting copies outbox columns into Kafka headers:

```json
"transforms.outbox.table.fields.additional.placement": "event_type:header:event_type,traceparent:header:traceparent,tracestate:header:tracestate"
```

So an outbox row becomes a Kafka message like:

```text
topic: order.events
key: <aggregate_id>
headers:
  event_type: OrderCreated
  traceparent: 00-...
  tracestate: ...
value:
  <payload json>
```

Kafka itself does not understand tracing. It only carries these headers as message metadata.

## Consumer Boundary

Kafka consumers extract trace context from message headers before handling the event.

The adapters convert Kafka headers into shared observability headers:

```go
observability.ContextFromMessageHeaders(ctx, headers)
```

Then they start consumer spans:

```text
analytics.consume OrderCreated
notification.consume OrderCreated
notification.consume CustomerCreated
```

Because the context was extracted from Kafka headers, these spans become children of the original outbox publish span, even though they run in different services.

## End to End Flow

For `POST /orders`, the trace should look like this:

```text
order-service
  POST /orders
    order.create
      inventory.reserve
      outbox.publish OrderCreated

analytics-consumer
  analytics.consume OrderCreated
    analytics.project_order_statistics

notification-service
  notification.consume OrderCreated
    notification.notify_order_created
    outbox.publish OrderNotificationSent
```

`OrderNotificationSent` events are currently ignored by `notification-service`, so they are committed but do not create a consumer span.

## What Correlates the Services

The correlation chain is:

```text
HTTP span context
  -> Go context.Context
  -> outbox_events.traceparent
  -> Kafka header traceparent
  -> consumer context.Context
  -> consumer span
```

All spans share the same `trace_id`.

Each new span has its own `span_id`, and the propagated `traceparent` tells the next service which span should be its parent.

## Inspecting Traces

Start the stack:

```bash
docker compose up -d --build
```

Create a customer and an order:

```bash
curl -sS -X POST http://localhost:8080/customers \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"customer-tracing-001"}'
```

```bash
curl -sS -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"customer-tracing-001","items":[{"sku":"BOOK-001","quantity":1}]}'
```

Open:

```text
http://localhost:16686
```

Search for service:

```text
order-service
```

Useful operations:

```text
POST /orders
outbox.publish OrderCreated
analytics.consume OrderCreated
notification.consume OrderCreated
```

## Common Issues

If a service does not appear in Jaeger, it has not exported any spans yet. Trigger one of the instrumented routes.

If only `order-service` appears, check that:

```text
outbox_events.traceparent
outbox_events.tracestate
```

are populated.

If outbox rows have trace context but consumers are not connected to the same trace, check Kafka headers in Kafka UI. Messages should contain:

```text
traceparent
tracestate
event_type
```

If migrations changed but the database was already created, reset the stack:

```bash
docker compose down -v --remove-orphans
docker compose up -d --build
```
