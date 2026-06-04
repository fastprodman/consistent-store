package entities

import (
	"time"

	"github.com/google/uuid"
)

const CustomerCreatedEventType = "CustomerCreated"

type CustomerCreatedEvent struct {
	id         uuid.UUID
	customerID string
	createdAt  time.Time
}

func NewCustomerCreatedEvent(id uuid.UUID, customerID string, createdAt time.Time) (CustomerCreatedEvent, error) {
	if id == uuid.Nil {
		return CustomerCreatedEvent{}, ErrEmptyEventID
	}

	if customerID == "" {
		return CustomerCreatedEvent{}, ErrEmptyCustomerID
	}

	if createdAt.IsZero() {
		return CustomerCreatedEvent{}, ErrEmptyCreatedAt
	}

	return CustomerCreatedEvent{
		id:         id,
		customerID: customerID,
		createdAt:  createdAt,
	}, nil
}

func (e CustomerCreatedEvent) ID() uuid.UUID {
	return e.id
}

func (e CustomerCreatedEvent) Type() string {
	return CustomerCreatedEventType
}

func (e CustomerCreatedEvent) CustomerID() string {
	return e.customerID
}

func (e CustomerCreatedEvent) CreatedAt() time.Time {
	return e.createdAt
}
