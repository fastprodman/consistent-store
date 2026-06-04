# Outbox to Kafka Mapping

This project uses the transactional outbox pattern. Application code writes a domain event into the `outbox_events` table in the same database transaction as the business change. Debezium reads that table from PostgreSQL WAL and publishes the event to Kafka.

## Outbox Table

The outbox table is defined in `migrations/001_init.sql`:

```sql
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSON NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Example row:

```text
id             = 9dfc4ec8-6d62-4d29-8c49-b92c0f8e5a61
aggregate_type = order
aggregate_id   = 5004bdd4-16b6-4e5a-ae6c-f66646e76124
event_type     = OrderCreated
payload         = {"event_id":"...","order_id":"..."}
```

## Debezium Connector

The connector is configured in `debezium/postgres-outbox-connector.json`.

Debezium watches only the outbox table:

```json
"table.include.list": "public.outbox_events"
```

The outbox transform is enabled here:

```json
"transforms": "outbox",
"transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter"
```

`EventRouter` turns each inserted outbox row into a Kafka message.

## Column to Kafka Mapping

| Outbox column | Kafka meaning | Debezium setting |
| --- | --- | --- |
| `aggregate_type` | Used to choose the Kafka topic | `transforms.outbox.route.by.field` |
| `aggregate_id` | Kafka message key | `transforms.outbox.table.field.event.key` |
| `payload` | Kafka message value | `transforms.outbox.table.field.event.payload` |
| `event_type` | Kafka message header named `event_type` | `transforms.outbox.table.fields.additional.placement` |
| `id` | Event id metadata/header used by EventRouter | `transforms.outbox.table.field.event.id` |

Relevant config:

```json
"transforms.outbox.table.field.event.id": "id",
"transforms.outbox.table.field.event.key": "aggregate_id",
"transforms.outbox.table.field.event.payload": "payload",
"transforms.outbox.table.field.event.type": "event_type",
"transforms.outbox.table.fields.additional.placement": "event_type:header:event_type",

"transforms.outbox.route.by.field": "aggregate_type",
"transforms.outbox.route.topic.replacement": "${routedByValue}.events",

"transforms.outbox.table.expand.json.payload": "true"
```

## Topic Routing

This setting chooses which outbox column routes the event:

```json
"transforms.outbox.route.by.field": "aggregate_type"
```

This setting defines the topic name pattern:

```json
"transforms.outbox.route.topic.replacement": "${routedByValue}.events"
```

So:

```text
aggregate_type = order    -> Kafka topic order.events
aggregate_type = customer -> Kafka topic customer.events
```

## Kafka Key

This setting makes `aggregate_id` the Kafka key:

```json
"transforms.outbox.table.field.event.key": "aggregate_id"
```

For an order event:

```text
Kafka key = order id
```

For a customer event:

```text
Kafka key = customer id
```

Kafka uses the key for partitioning, so events for the same aggregate go to the same partition and preserve order relative to that aggregate.

## Kafka Headers

This setting places the outbox `event_type` column into a Kafka message header:

```json
"transforms.outbox.table.fields.additional.placement": "event_type:header:event_type"
```

The syntax is:

```text
<outbox column>:header:<Kafka header name>
```

So:

```text
outbox_events.event_type -> Kafka header event_type
```

For example:

```text
event_type = OrderCreated
```

becomes:

```text
Kafka header event_type = OrderCreated
```

The event type is intentionally not duplicated inside the JSON message body.

## Kafka Value

This setting selects the outbox payload:

```json
"transforms.outbox.table.field.event.payload": "payload"
```

This setting expands the JSON payload instead of wrapping it as an escaped JSON string:

```json
"transforms.outbox.table.expand.json.payload": "true"
```

So if `payload` is:

```json
{
  "event_id": "9dfc4ec8-6d62-4d29-8c49-b92c0f8e5a61",
  "order_id": "5004bdd4-16b6-4e5a-ae6c-f66646e76124",
  "customer_id": "customer-001",
  "total_cents": 3000
}
```

Then the Kafka message value is that JSON object.

## End to End Examples

Order event outbox row:

```text
aggregate_type = order
aggregate_id   = 5004bdd4-16b6-4e5a-ae6c-f66646e76124
event_type     = OrderCreated
payload         = {...}
```

Kafka message:

```text
topic = order.events
key   = 5004bdd4-16b6-4e5a-ae6c-f66646e76124
header event_type = OrderCreated
value = payload JSON
```

Customer event outbox row:

```text
aggregate_type = customer
aggregate_id   = customer-001
event_type     = CustomerCreated
payload         = {...}
```

Kafka message:

```text
topic = customer.events
key   = customer-001
header event_type = CustomerCreated
value = payload JSON
```

## Consumer Side Event Type Filtering

Kafka topic routing is intentionally broad in this project.

For example, all order aggregate events go to:

```text
order.events
```

That topic can contain different event types:

```text
OrderCreated
OrderNotificationSent
```

Kafka only delivers messages from the topic. It does not decide which domain handler should process each event type. That dispatch is done by the consumer code.

For example, `notification-service` subscribes to both:

```text
order.events
customer.events
```

Then it reads the Kafka `event_type` header and chooses what to do:

```text
topic = order.events
header event_type = OrderCreated
-> send/log order notification
```

```text
topic = order.events
header event_type = OrderNotificationSent
-> ignore, because notification-service emitted this event itself
```

```text
topic = customer.events
header event_type = CustomerCreated
-> send/log customer notification
```

This is why consumers should not assume that every message in `order.events` has the same JSON shape. The topic tells the consumer the aggregate stream; `event_type` tells it the specific event contract.

In code, this is the responsibility of the inbound Kafka adapter:

```text
internal/domains/notification/adapters/in/kafka.go
```

The adapter first dispatches by topic, then by `event_type`.
