# Runbook

This file shows how to start the local stack and verify the transactional outbox flow.

## Start Services

Start everything:

```bash
docker compose up -d --build --remove-orphans
```

Watch startup logs:

```bash
docker compose logs -f postgres kafka-1 kafka-2 kafka-3 kafka-connect debezium-init order-service analytics-consumer notification-service
```

Check service status:

```bash
docker compose ps
```

Kafka UI is available at:

```text
http://localhost:8081
```

Order service is available at:

```text
http://localhost:8080
```

Kafka Connect is available at:

```text
http://localhost:8083
```

## Clean Reset

Use this when changing migrations or wanting a fresh database:

```bash
docker compose down -v --remove-orphans
docker compose up -d --build
```

`-v` removes the Postgres volume, so migrations in `migrations/` run again.

## Check Debezium

Confirm the connector exists and is running:

```bash
curl -sS http://localhost:8083/connectors/postgres-outbox-connector/status
```

Check Postgres logical replication slot:

```bash
docker compose exec postgres psql -U app -d outbox_demo -c "SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots;"
```

Expected slot:

```text
outbox_demo_slot
```

## Create Customer

Create a customer with a generated ID:

```bash
customer=$(curl -sS -X POST http://localhost:8080/customers \
  -H "Content-Type: application/json" \
  -d '{}')

echo "$customer"
```

Extract the customer ID:

```bash
customer_id=$(echo "$customer" | jq -r '.customer_id')
echo "$customer_id"
```

Or create a customer with an explicit ID:

```bash
curl -sS -X POST http://localhost:8080/customers \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"customer-002"}'
```

Observe customer-created outbox event:

```bash
docker compose exec postgres psql -U app -d outbox_demo -c "SELECT aggregate_type, aggregate_id, event_type, payload, created_at FROM outbox_events ORDER BY created_at DESC LIMIT 5;"
```

Expected:

```text
aggregate_type = customer
event_type     = CustomerCreated
```

Observe notification-service log:

```bash
docker compose logs notification-service --tail=50
```

Expected:

```text
customer notification sent
```

## Check Inventory

```bash
curl -sS http://localhost:8080/inventory/BOOK-001
```

Expected initial quantity after a fresh reset:

```json
{"sku":"BOOK-001","available_quantity":10,"price_cents":1500}
```

## Create Order

Create an order for the customer:

```bash
body=$(cat <<EOF
{
  "customer_id": "$customer_id",
  "items": [
    {
      "sku": "BOOK-001",
      "quantity": 2
    }
  ]
}
EOF
)

order=$(curl -sS -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d "$body")

echo "$order"
```

Extract order ID:

```bash
order_id=$(echo "$order" | jq -r '.order_id')
echo "$order_id"
```

Fetch the order:

```bash
curl -sS "http://localhost:8080/orders/$order_id"
```

Observe inventory decreased:

```bash
curl -sS http://localhost:8080/inventory/BOOK-001
```

Expected quantity decreased by `2`.

## Observe Outbox

```bash
docker compose exec postgres psql -U app -d outbox_demo -c "SELECT aggregate_type, aggregate_id, event_type, payload, created_at FROM outbox_events ORDER BY created_at DESC LIMIT 10;"
```

Expected event types:

```text
CustomerCreated
OrderCreated
OrderNotificationSent
```

Debezium routes:

```text
aggregate_type = customer -> customer.events
aggregate_type = order    -> order.events
```

`event_type` is emitted as a Kafka header named `event_type`.

## Observe Notification Result

Order notifications are recorded in `notification_log`:

```bash
docker compose exec postgres psql -U app -d outbox_demo -c "SELECT event_id, order_id, customer_id, status, processed_at FROM notification_log ORDER BY processed_at DESC LIMIT 5;"
```

Expected:

```text
status = sent
```

Notification-service also emits `OrderNotificationSent` into the outbox after a new order notification is recorded.

## Observe Analytics Result

Analytics consumer updates daily order analytics:

```bash
docker compose exec postgres psql -U app -d outbox_demo -c "SELECT date, order_count, revenue_cents FROM order_analytics ORDER BY date DESC LIMIT 5;"
```

Expected after ordering two `BOOK-001` items:

```text
order_count   = 1
revenue_cents = 3000
```

Analytics idempotency is tracked in `processed_events`:

```bash
docker compose exec postgres psql -U app -d outbox_demo -c "SELECT consumer_name, event_id, processed_at FROM processed_events ORDER BY processed_at DESC LIMIT 5;"
```

Expected:

```text
consumer_name = analytics-consumer
```

## Observe Kafka Topics

List topics:

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --list
```

Describe order topic:

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic order.events
```

Describe customer topic:

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic customer.events
```

Expected replication factor:

```text
ReplicationFactor: 3
```

## Common Issues

If inventory decreases but analytics/notifications do not update, check Debezium:

```bash
curl -sS http://localhost:8083/connectors/postgres-outbox-connector/status
docker compose logs kafka-connect debezium-init --tail=100
```

If consumers cannot connect to Kafka, rebuild recreated images after broker config changes:

```bash
docker compose up -d --build --force-recreate analytics-consumer notification-service
```

If migrations changed but database shape did not, reset volumes:

```bash
docker compose down -v --remove-orphans
docker compose up -d --build
```

