package outbox

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/db"
)

type Repository struct {
	db *db.Provider
}

func NewRepository(db *db.Provider) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Add(ctx context.Context, event Event) error {
	exec := r.db.Executor(ctx)

	_, err := exec.ExecContext(ctx, `
		INSERT INTO outbox_events (
			id,
			aggregate_type,
			aggregate_id,
			event_type,
			payload
		)
		VALUES ($1, $2, $3, $4, $5)
	`,
		event.ID,
		event.AggregateType,
		event.AggregateID,
		event.EventType,
		event.Payload,
	)

	return err
}
