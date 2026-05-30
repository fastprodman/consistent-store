package entities

import (
	"time"

	"github.com/google/uuid"
)

const OrderCreatedEventType = "OrderCreated"

type OrderCreatedEvent struct {
	id         uuid.UUID
	orderID    uuid.UUID
	customerID string
	items      []OrderCreatedEventItem
	totalCents int
	createdAt  time.Time
}

type OrderCreatedEventItem struct {
	sku        string
	quantity   int
	priceCents int
}

func NewOrderCreatedEvent(
	id uuid.UUID,
	orderID uuid.UUID,
	customerID string,
	items []OrderCreatedEventItem,
	totalCents int,
	createdAt time.Time,
) OrderCreatedEvent {
	return OrderCreatedEvent{
		id:         id,
		orderID:    orderID,
		customerID: customerID,
		items:      append([]OrderCreatedEventItem(nil), items...),
		totalCents: totalCents,
		createdAt:  createdAt,
	}
}

func NewOrderCreatedEventItem(sku string, quantity int, priceCents int) OrderCreatedEventItem {
	return OrderCreatedEventItem{
		sku:        sku,
		quantity:   quantity,
		priceCents: priceCents,
	}
}

func (e OrderCreatedEvent) ID() uuid.UUID {
	return e.id
}

func (e OrderCreatedEvent) Type() string {
	return OrderCreatedEventType
}

func (e OrderCreatedEvent) OrderID() uuid.UUID {
	return e.orderID
}

func (e OrderCreatedEvent) CustomerID() string {
	return e.customerID
}

func (e OrderCreatedEvent) Items() []OrderCreatedEventItem {
	return append([]OrderCreatedEventItem(nil), e.items...)
}

func (e OrderCreatedEvent) TotalCents() int {
	return e.totalCents
}

func (e OrderCreatedEvent) CreatedAt() time.Time {
	return e.createdAt
}

func (i OrderCreatedEventItem) SKU() string {
	return i.sku
}

func (i OrderCreatedEventItem) Quantity() int {
	return i.quantity
}

func (i OrderCreatedEventItem) PriceCents() int {
	return i.priceCents
}
