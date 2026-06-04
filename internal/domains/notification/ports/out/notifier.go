package out

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/notification/entities"
)

type Notifier interface {
	NotifyOrderCreated(ctx context.Context, event entities.OrderCreatedEvent) (notified bool, err error)
	NotifyCustomerCreated(ctx context.Context, event entities.CustomerCreatedEvent) error
}
