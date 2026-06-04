package out

import (
	"context"
	"database/sql"
	"errors"
	"log"

	"github.com/fastprodman/consistent-store/internal/domains/notification/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/notification/ports/out"
	"github.com/fastprodman/consistent-store/internal/shared/db"
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
			return false, nil
		}

		return false, err
	}

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
