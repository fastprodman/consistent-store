package out

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/order/entities"
	"github.com/google/uuid"
)

type Repository interface {
	Create(ctx context.Context, order entities.Order) error
	CreateItem(ctx context.Context, item entities.OrderItem) error
	Get(ctx context.Context, id uuid.UUID) (entities.Order, error)
	GetItems(ctx context.Context, orderID uuid.UUID) ([]entities.OrderItem, error)
}
