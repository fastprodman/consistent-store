package out

import (
	"context"
	"database/sql"
	"errors"

	"github.com/fastprodman/consistent-store/internal/db"
	"github.com/fastprodman/consistent-store/internal/domains/analytics/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/analytics/ports/out"
)

const consumerName = "analytics-consumer"

type Projector struct {
	db *db.Provider
}

var _ portsout.Projector = (*Projector)(nil)

func NewProjector(db *db.Provider) *Projector {
	return &Projector{db: db}
}

func (p *Projector) ApplyOrderCreated(
	ctx context.Context,
	event entities.OrderCreatedEvent,
) (applied bool, err error) {
	exec := p.db.Executor(ctx)

	var didInsert bool

	err = exec.QueryRowContext(ctx, `
		INSERT INTO processed_events (
			consumer_name,
			event_id
		)
		VALUES ($1, $2)
		ON CONFLICT (consumer_name, event_id) DO NOTHING
		RETURNING true
	`, consumerName, event.EventID()).Scan(&didInsert)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	analyticsDate := event.CreatedAt().UTC().Format("2006-01-02")

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
	`, analyticsDate, event.TotalCents())

	if err != nil {
		return false, err
	}

	return true, nil
}
