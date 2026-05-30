package services

import (
	"context"

	portsout "github.com/fastprodman/consistent-store/internal/domains/inventory/ports/out"
)

type ReservationService struct {
	inventory portsout.Repository
}

func NewReservationService(inventory portsout.Repository) *ReservationService {
	return &ReservationService{
		inventory: inventory,
	}
}

func (s *ReservationService) Reserve(ctx context.Context, sku string, quantity int) (priceCents int, err error) {
	return s.inventory.Reserve(ctx, sku, quantity)
}
