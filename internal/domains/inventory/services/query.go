package services

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/inventory/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/inventory/ports/out"
)

type QueryService struct {
	inventory portsout.Repository
}

func NewQueryService(inventory portsout.Repository) *QueryService {
	return &QueryService{
		inventory: inventory,
	}
}

func (s *QueryService) GetInventory(ctx context.Context, sku string) (entities.Item, error) {
	return s.inventory.Get(ctx, sku)
}
