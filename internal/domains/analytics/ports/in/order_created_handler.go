package in

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/analytics/entities"
)

type OrderCreatedHandler interface {
	HandleOrderCreated(ctx context.Context, event entities.OrderCreatedEvent) (applied bool, err error)
}
