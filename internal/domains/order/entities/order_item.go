package entities

import "github.com/google/uuid"

type OrderItem struct {
	id         uuid.UUID
	orderID    uuid.UUID
	sku        string
	quantity   int
	priceCents int
}

func NewOrderItem(id uuid.UUID, orderID uuid.UUID, sku string, quantity int, priceCents int) OrderItem {
	return OrderItem{
		id:         id,
		orderID:    orderID,
		sku:        sku,
		quantity:   quantity,
		priceCents: priceCents,
	}
}

func (i OrderItem) ID() uuid.UUID {
	return i.id
}

func (i OrderItem) OrderID() uuid.UUID {
	return i.orderID
}

func (i OrderItem) SKU() string {
	return i.sku
}

func (i OrderItem) Quantity() int {
	return i.quantity
}

func (i OrderItem) PriceCents() int {
	return i.priceCents
}
