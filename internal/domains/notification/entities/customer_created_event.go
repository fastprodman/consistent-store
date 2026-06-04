package entities

import "github.com/google/uuid"

type CustomerCreatedEvent struct {
	eventID    uuid.UUID
	customerID string
}

func NewCustomerCreatedEvent(
	eventID uuid.UUID,
	customerID string,
) (CustomerCreatedEvent, error) {
	if eventID == uuid.Nil {
		return CustomerCreatedEvent{}, ErrEventIDRequired
	}

	if customerID == "" {
		return CustomerCreatedEvent{}, ErrCustomerIDRequired
	}

	return CustomerCreatedEvent{
		eventID:    eventID,
		customerID: customerID,
	}, nil
}

func (e CustomerCreatedEvent) EventID() uuid.UUID {
	return e.eventID
}

func (e CustomerCreatedEvent) CustomerID() string {
	return e.customerID
}
