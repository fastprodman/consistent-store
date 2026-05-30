package in

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/ordernotifier/entities"
)

type OrderCreatedHandler interface {
	HandleOrderCreated(ctx context.Context, event entities.OrderCreatedEvent) (notified bool, err error)
}
