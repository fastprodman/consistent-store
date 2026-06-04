package events

import (
	"time"

	"github.com/google/uuid"
)

type OrderNotificationSent struct {
	EventID    uuid.UUID `json:"event_id"`
	OrderID    uuid.UUID `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	SentAt     time.Time `json:"sent_at"`
}
