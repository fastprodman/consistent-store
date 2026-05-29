package events

import (
	"time"

	"github.com/google/uuid"
)

type OrderCreated struct {
	EventID    uuid.UUID          `json:"event_id"`
	EventType  string             `json:"event_type"`
	OrderID    uuid.UUID          `json:"order_id"`
	CustomerID string             `json:"customer_id"`
	Items      []OrderCreatedItem `json:"items"`
	TotalCents int                `json:"total_cents"`
	CreatedAt  time.Time          `json:"created_at"`
}

type OrderCreatedItem struct {
	SKU        string `json:"sku"`
	Quantity   int    `json:"quantity"`
	PriceCents int    `json:"price_cents"`
}
