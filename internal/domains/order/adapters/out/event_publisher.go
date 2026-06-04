package out

import (
	"context"
	"encoding/json"
	"time"

	"github.com/fastprodman/consistent-store/internal/domains/order/entities"
	"github.com/fastprodman/consistent-store/internal/shared/db"
	"github.com/google/uuid"
)

const orderAggregateType = "order"

type OutboxEventPublisher struct {
	db *db.Provider
}

func NewOutboxEventPublisher(db *db.Provider) *OutboxEventPublisher {
	return &OutboxEventPublisher{db: db}
}

func (p *OutboxEventPublisher) PublishOrderCreated(ctx context.Context, event entities.OrderCreatedEvent) error {
	payload, err := json.Marshal(orderCreatedPayload{
		EventID:    event.ID(),
		OrderID:    event.OrderID(),
		CustomerID: event.CustomerID(),
		Items:      newOrderCreatedItemPayloads(event.Items()),
		TotalCents: event.TotalCents(),
		CreatedAt:  event.CreatedAt(),
	})
	if err != nil {
		return err
	}

	exec := p.db.Executor(ctx)

	_, err = exec.ExecContext(ctx, `
		INSERT INTO outbox_events (
			id,
			aggregate_type,
			aggregate_id,
			event_type,
			payload
		)
		VALUES ($1, $2, $3, $4, $5)
	`,
		event.ID(),
		orderAggregateType,
		event.OrderID(),
		event.Type(),
		payload,
	)

	return err
}

func newOrderCreatedItemPayloads(items []entities.OrderCreatedEventItem) []orderCreatedItemPayload {
	payloads := make([]orderCreatedItemPayload, 0, len(items))

	for _, item := range items {
		payloads = append(payloads, orderCreatedItemPayload{
			SKU:        item.SKU(),
			Quantity:   item.Quantity(),
			PriceCents: item.PriceCents(),
		})
	}

	return payloads
}

type orderCreatedPayload struct {
	EventID    uuid.UUID                 `json:"event_id"`
	OrderID    uuid.UUID                 `json:"order_id"`
	CustomerID string                    `json:"customer_id"`
	Items      []orderCreatedItemPayload `json:"items"`
	TotalCents int                       `json:"total_cents"`
	CreatedAt  time.Time                 `json:"created_at"`
}

type orderCreatedItemPayload struct {
	SKU        string `json:"sku"`
	Quantity   int    `json:"quantity"`
	PriceCents int    `json:"price_cents"`
}
