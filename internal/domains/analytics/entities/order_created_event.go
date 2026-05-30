package entities

import (
	"time"

	"github.com/google/uuid"
)

type OrderCreatedEvent struct {
	eventID    uuid.UUID
	orderID    uuid.UUID
	totalCents int
	createdAt  time.Time
}

func NewOrderCreatedEvent(
	eventID uuid.UUID,
	orderID uuid.UUID,
	totalCents int,
	createdAt time.Time,
) (OrderCreatedEvent, error) {
	if eventID == uuid.Nil {
		return OrderCreatedEvent{}, ErrEventIDRequired
	}

	if orderID == uuid.Nil {
		return OrderCreatedEvent{}, ErrOrderIDRequired
	}

	if totalCents <= 0 {
		return OrderCreatedEvent{}, ErrTotalCentsPositive
	}

	if createdAt.IsZero() {
		return OrderCreatedEvent{}, ErrCreatedAtIsRequired
	}

	return OrderCreatedEvent{
		eventID:    eventID,
		orderID:    orderID,
		totalCents: totalCents,
		createdAt:  createdAt,
	}, nil
}

func (e OrderCreatedEvent) EventID() uuid.UUID {
	return e.eventID
}

func (e OrderCreatedEvent) OrderID() uuid.UUID {
	return e.orderID
}

func (e OrderCreatedEvent) TotalCents() int {
	return e.totalCents
}

func (e OrderCreatedEvent) CreatedAt() time.Time {
	return e.createdAt
}
