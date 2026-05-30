package out

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/analytics/entities"
)

type Projector interface {
	ApplyOrderCreated(ctx context.Context, event entities.OrderCreatedEvent) (applied bool, err error)
}
