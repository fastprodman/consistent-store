package in

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/notification/entities"
)

type OrderCreatedHandler interface {
	HandleOrderCreated(ctx context.Context, event entities.OrderCreatedEvent) (notified bool, err error)
}

type CustomerCreatedHandler interface {
	HandleCustomerCreated(ctx context.Context, event entities.CustomerCreatedEvent) error
}
