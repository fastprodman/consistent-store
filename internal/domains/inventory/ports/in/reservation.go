package in

import "context"

type ReservationService interface {
	Reserve(ctx context.Context, sku string, quantity int) (priceCents int, err error)
}
