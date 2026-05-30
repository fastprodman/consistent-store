package out

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/order/entities"
)

type EventPublisher interface {
	PublishOrderCreated(ctx context.Context, event entities.OrderCreatedEvent) error
}
