package entities

import "github.com/google/uuid"

type OrderCreatedEvent struct {
	eventID    uuid.UUID
	orderID    uuid.UUID
	customerID string
}

func NewOrderCreatedEvent(
	eventID uuid.UUID,
	orderID uuid.UUID,
	customerID string,
) (OrderCreatedEvent, error) {
	if eventID == uuid.Nil {
		return OrderCreatedEvent{}, ErrEventIDRequired
	}

	if orderID == uuid.Nil {
		return OrderCreatedEvent{}, ErrOrderIDRequired
	}

	if customerID == "" {
		return OrderCreatedEvent{}, ErrCustomerIDRequired
	}

	return OrderCreatedEvent{
		eventID:    eventID,
		orderID:    orderID,
		customerID: customerID,
	}, nil
}

func (e OrderCreatedEvent) EventID() uuid.UUID {
	return e.eventID
}

func (e OrderCreatedEvent) OrderID() uuid.UUID {
	return e.orderID
}

func (e OrderCreatedEvent) CustomerID() string {
	return e.customerID
}
