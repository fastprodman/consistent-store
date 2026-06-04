package events

import (
	"time"

	"github.com/google/uuid"
)

type CustomerCreated struct {
	EventID    uuid.UUID `json:"event_id"`
	CustomerID string    `json:"customer_id"`
	CreatedAt  time.Time `json:"created_at"`
}
