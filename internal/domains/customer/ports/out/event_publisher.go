package out

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/customer/entities"
)

type EventPublisher interface {
	PublishCustomerCreated(ctx context.Context, event entities.CustomerCreatedEvent) error
}
