package out

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/inventory/entities"
)

type Repository interface {
	Reserve(ctx context.Context, sku string, quantity int) (priceCents int, err error)
	Get(ctx context.Context, sku string) (entities.Item, error)
}
