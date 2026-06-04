package entities

import (
	"time"

	"github.com/google/uuid"
)

const OrderNotificationSentEventType = "OrderNotificationSent"

type OrderNotificationSentEvent struct {
	eventID    uuid.UUID
	orderID    uuid.UUID
	customerID string
	sentAt     time.Time
}

func NewOrderNotificationSentEvent(
	eventID uuid.UUID,
	orderID uuid.UUID,
	customerID string,
	sentAt time.Time,
) (OrderNotificationSentEvent, error) {
	if eventID == uuid.Nil {
		return OrderNotificationSentEvent{}, ErrEventIDRequired
	}

	if orderID == uuid.Nil {
		return OrderNotificationSentEvent{}, ErrOrderIDRequired
	}

	if customerID == "" {
		return OrderNotificationSentEvent{}, ErrCustomerIDRequired
	}

	return OrderNotificationSentEvent{
		eventID:    eventID,
		orderID:    orderID,
		customerID: customerID,
		sentAt:     sentAt,
	}, nil
}

func (e OrderNotificationSentEvent) EventID() uuid.UUID {
	return e.eventID
}

func (e OrderNotificationSentEvent) EventType() string {
	return OrderNotificationSentEventType
}

func (e OrderNotificationSentEvent) OrderID() uuid.UUID {
	return e.orderID
}

func (e OrderNotificationSentEvent) CustomerID() string {
	return e.customerID
}

func (e OrderNotificationSentEvent) SentAt() time.Time {
	return e.sentAt
}
