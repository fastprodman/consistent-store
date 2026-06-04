package out

import (
	"context"
	"encoding/json"

	"github.com/fastprodman/consistent-store/internal/domains/notification/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/notification/ports/out"
	"github.com/fastprodman/consistent-store/internal/shared/db"
	"github.com/fastprodman/consistent-store/internal/shared/events"
)

const orderAggregateType = "order"

type OutboxEventPublisher struct {
	db *db.Provider
}

var _ portsout.EventPublisher = (*OutboxEventPublisher)(nil)

func NewOutboxEventPublisher(db *db.Provider) *OutboxEventPublisher {
	return &OutboxEventPublisher{db: db}
}

func (p *OutboxEventPublisher) PublishOrderNotificationSent(
	ctx context.Context,
	event entities.OrderNotificationSentEvent,
) error {
	payload, err := json.Marshal(events.OrderNotificationSent{
		EventID:    event.EventID(),
		OrderID:    event.OrderID(),
		CustomerID: event.CustomerID(),
		SentAt:     event.SentAt(),
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
		event.EventID(),
		orderAggregateType,
		event.OrderID().String(),
		event.EventType(),
		payload,
	)

	return err
}
