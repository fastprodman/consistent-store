package out

import (
	"context"
	"encoding/json"

	"github.com/fastprodman/consistent-store/internal/domains/notification/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/notification/ports/out"
	"github.com/fastprodman/consistent-store/internal/shared/db"
	"github.com/fastprodman/consistent-store/internal/shared/events"
	"github.com/fastprodman/consistent-store/internal/shared/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	ctx, span := otel.Tracer("notification.outbox").Start(
		ctx,
		"outbox.publish OrderNotificationSent",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("event.type", event.EventType()),
			attribute.String("messaging.destination.name", orderAggregateType+".events"),
			attribute.String("outbox.aggregate_type", orderAggregateType),
			attribute.String("outbox.aggregate_id", event.OrderID().String()),
			attribute.String("outbox.event_id", event.EventID().String()),
		),
	)
	defer span.End()

	payload, err := json.Marshal(events.OrderNotificationSent{
		EventID:    event.EventID(),
		OrderID:    event.OrderID(),
		CustomerID: event.CustomerID(),
		SentAt:     event.SentAt(),
	})
	if err != nil {
		recordSpanError(span, err)
		return err
	}

	exec := p.db.Executor(ctx)
	traceHeaders := observability.TraceHeadersFromContext(ctx)

	_, err = exec.ExecContext(ctx, `
		INSERT INTO outbox_events (
			id,
			aggregate_type,
			aggregate_id,
			event_type,
			payload,
			traceparent,
			tracestate
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`,
		event.EventID(),
		orderAggregateType,
		event.OrderID().String(),
		event.EventType(),
		payload,
		traceHeaders.Traceparent,
		traceHeaders.Tracestate,
	)

	if err != nil {
		recordSpanError(span, err)
	}

	return err
}

func recordSpanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
