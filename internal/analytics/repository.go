package analytics

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/fastprodman/consistent-store/internal/db"
	"github.com/google/uuid"
)

const ConsumerName = "analytics-consumer"

type Repository struct {
	db *db.Provider
}

func NewRepository(db *db.Provider) *Repository {
	return &Repository{db: db}
}

func (r *Repository) ApplyOrderCreated(
	ctx context.Context,
	eventID uuid.UUID,
	createdAt time.Time,
	totalCents int,
) (inserted bool, err error) {
	exec := r.db.Executor(ctx)

	var didInsert bool

	err = exec.QueryRowContext(ctx, `
		INSERT INTO processed_events (
			consumer_name,
			event_id
		)
		VALUES ($1, $2)
		ON CONFLICT (consumer_name, event_id) DO NOTHING
		RETURNING true
	`, ConsumerName, eventID).Scan(&didInsert)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	analyticsDate := createdAt.UTC().Format("2006-01-02")

	_, err = exec.ExecContext(ctx, `
		INSERT INTO order_analytics (
			date,
			order_count,
			revenue_cents
		)
		VALUES ($1, 1, $2)
		ON CONFLICT (date)
		DO UPDATE SET
			order_count = order_analytics.order_count + 1,
			revenue_cents = order_analytics.revenue_cents + EXCLUDED.revenue_cents
	`, analyticsDate, totalCents)

	if err != nil {
		return false, err
	}

	return true, nil
}
