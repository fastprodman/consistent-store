package entities

import "github.com/google/uuid"

type OrderStatus string

const (
	OrderStatusCreated OrderStatus = "created"
)

type Order struct {
	id         uuid.UUID
	customerID string
	status     OrderStatus
	totalCents int
}

func NewOrder(id uuid.UUID, customerID string, status OrderStatus, totalCents int) Order {
	return Order{
		id:         id,
		customerID: customerID,
		status:     status,
		totalCents: totalCents,
	}
}

func NewCreatedOrder(id uuid.UUID, customerID string, totalCents int) Order {
	return NewOrder(id, customerID, OrderStatusCreated, totalCents)
}

func (o Order) ID() uuid.UUID {
	return o.id
}

func (o Order) CustomerID() string {
	return o.customerID
}

func (o Order) Status() OrderStatus {
	return o.status
}

func (o Order) TotalCents() int {
	return o.totalCents
}
