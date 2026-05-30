package in

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/inventory/entities"
)

type QueryService interface {
	GetInventory(ctx context.Context, sku string) (entities.Item, error)
}
