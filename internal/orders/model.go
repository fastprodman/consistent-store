package orders

import "github.com/google/uuid"

type Order struct {
	ID         uuid.UUID
	CustomerID string
	Status     string
	TotalCents int
}

type OrderItem struct {
	ID         uuid.UUID
	OrderID    uuid.UUID
	SKU        string
	Quantity   int
	PriceCents int
}

type CreateOrderCommand struct {
	CustomerID string
	Items      []CreateOrderItem
}

type CreateOrderItem struct {
	SKU      string
	Quantity int
}
