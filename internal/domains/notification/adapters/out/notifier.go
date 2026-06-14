package out

import (
	"context"
	"database/sql"
	"errors"
	"log"

	"github.com/fastprodman/consistent-store/internal/domains/notification/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/notification/ports/out"
	"github.com/fastprodman/consistent-store/internal/shared/db"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Notifier struct {
	db *db.Provider
}

var _ portsout.Notifier = (*Notifier)(nil)

func NewNotifier(db *db.Provider) *Notifier {
	return &Notifier{db: db}
}

func (n *Notifier) NotifyOrderCreated(
	ctx context.Context,
	event entities.OrderCreatedEvent,
) (notified bool, err error) {
	ctx, span := otel.Tracer("notification.notifier").Start(
		ctx,
		"notification.notify_order_created",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "insert"),
			attribute.String("event.id", event.EventID().String()),
			attribute.String("order.id", event.OrderID().String()),
			attribute.String("customer.id", event.CustomerID()),
		),
	)
	defer span.End()

	// This adapter is where real SMS, email, or other notification providers
	// could be called. In this lab it records the notification in the database.
	exec := n.db.Executor(ctx)

	var didInsert bool

	err = exec.QueryRowContext(ctx, `
		INSERT INTO notification_log (
			event_id,
			order_id,
			customer_id,
			status
		)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING true
	`, event.EventID(), event.OrderID(), event.CustomerID(), "sent").Scan(&didInsert)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			span.SetAttributes(attribute.Bool("notification.sent", false))
			return false, nil
		}

		recordSpanError(span, err)
		return false, err
	}

	span.SetAttributes(attribute.Bool("notification.sent", didInsert))

	return didInsert, nil
}

func (n *Notifier) NotifyCustomerCreated(ctx context.Context, event entities.CustomerCreatedEvent) error {
	log.Printf(
		"customer notification sent: event_id=%s customer_id=%s",
		event.EventID(),
		event.CustomerID(),
	)

	return nil
}
