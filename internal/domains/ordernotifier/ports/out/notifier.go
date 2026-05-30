package out

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/ordernotifier/entities"
)

type Notifier interface {
	NotifyOrderCreated(ctx context.Context, event entities.OrderCreatedEvent) (notified bool, err error)
}
