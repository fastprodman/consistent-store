package out

import (
	"context"
	"database/sql"
	"errors"
	"log"

	"github.com/fastprodman/consistent-store/internal/domains/analytics/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/analytics/ports/out"
	"github.com/fastprodman/consistent-store/internal/shared/db"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	ctx, span := otel.Tracer("analytics.projector").Start(
		ctx,
		"analytics.project_order_statistics",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "upsert"),
			attribute.String("event.id", event.EventID().String()),
			attribute.String("order.id", event.OrderID().String()),
			attribute.Int("order.total_cents", event.TotalCents()),
		),
	)
	defer span.End()

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
			span.SetAttributes(attribute.Bool("analytics.applied", false))
			return false, nil
		}

		recordSpanError(span, err)
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
		recordSpanError(span, err)
		return false, err
	}

	span.SetAttributes(
		attribute.Bool("analytics.applied", true),
		attribute.String("analytics.date", analyticsDate),
	)

	log.Printf(
		"order analytics statistics updated: event_id=%s order_id=%s date=%s order_count_delta=1 revenue_cents_delta=%d",
		event.EventID(),
		event.OrderID(),
		analyticsDate,
		event.TotalCents(),
	)

	return true, nil
}

func recordSpanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
