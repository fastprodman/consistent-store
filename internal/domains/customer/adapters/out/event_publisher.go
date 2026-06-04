package out

import (
	"context"
	"encoding/json"
	"time"

	"github.com/fastprodman/consistent-store/internal/domains/customer/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/customer/ports/out"
	"github.com/fastprodman/consistent-store/internal/shared/db"
	"github.com/google/uuid"
)

const customerAggregateType = "customer"

type OutboxEventPublisher struct {
	db *db.Provider
}

var _ portsout.EventPublisher = (*OutboxEventPublisher)(nil)

func NewOutboxEventPublisher(db *db.Provider) *OutboxEventPublisher {
	return &OutboxEventPublisher{db: db}
}

func (p *OutboxEventPublisher) PublishCustomerCreated(ctx context.Context, event entities.CustomerCreatedEvent) error {
	payload, err := json.Marshal(customerCreatedPayload{
		EventID:    event.ID(),
		CustomerID: event.CustomerID(),
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
		customerAggregateType,
		event.CustomerID(),
		event.Type(),
		payload,
	)

	return err
}

type customerCreatedPayload struct {
	EventID    uuid.UUID `json:"event_id"`
	CustomerID string    `json:"customer_id"`
	CreatedAt  time.Time `json:"created_at"`
}
