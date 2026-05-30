package in

import (
	"context"

	"github.com/google/uuid"
)

type Service interface {
	CreateOrder(ctx context.Context, cmd CreateOrderCommand) (uuid.UUID, error)
}

type CreateOrderCommand struct {
	CustomerID string
	Items      []CreateOrderItem
}

type CreateOrderItem struct {
	SKU      string
	Quantity int
}
