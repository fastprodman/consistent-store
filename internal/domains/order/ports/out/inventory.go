package out

import "context"

type Inventory interface {
	Reserve(ctx context.Context, sku string, quantity int) (priceCents int, err error)
}
