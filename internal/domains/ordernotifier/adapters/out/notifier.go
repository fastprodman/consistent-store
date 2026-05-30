package out

import (
	"context"
	"database/sql"
	"errors"

	"github.com/fastprodman/consistent-store/internal/db"
	"github.com/fastprodman/consistent-store/internal/domains/ordernotifier/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/ordernotifier/ports/out"
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
