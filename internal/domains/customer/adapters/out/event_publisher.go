package out

import (
	"context"
	"encoding/json"
	"time"

	"github.com/fastprodman/consistent-store/internal/domains/customer/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/customer/ports/out"
	"github.com/fastprodman/consistent-store/internal/shared/db"
	"github.com/fastprodman/consistent-store/internal/shared/observability"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	ctx, span := otel.Tracer("customer.outbox").Start(
		ctx,
		"outbox.publish CustomerCreated",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("event.type", event.Type()),
			attribute.String("messaging.destination.name", customerAggregateType+".events"),
			attribute.String("outbox.aggregate_type", customerAggregateType),
			attribute.String("outbox.aggregate_id", event.CustomerID()),
			attribute.String("outbox.event_id", event.ID().String()),
		),
	)
	defer span.End()

	payload, err := json.Marshal(customerCreatedPayload{
		EventID:    event.ID(),
		CustomerID: event.CustomerID(),
		CreatedAt:  event.CreatedAt(),
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
		event.ID(),
		customerAggregateType,
		event.CustomerID(),
		event.Type(),
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

type customerCreatedPayload struct {
	EventID    uuid.UUID `json:"event_id"`
	CustomerID string    `json:"customer_id"`
	CreatedAt  time.Time `json:"created_at"`
}
