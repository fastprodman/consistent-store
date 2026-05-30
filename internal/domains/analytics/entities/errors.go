package entities

import "errors"

var (
	ErrEventIDRequired     = errors.New("event_id is required")
	ErrOrderIDRequired     = errors.New("order_id is required")
	ErrTotalCentsPositive  = errors.New("total_cents must be positive")
	ErrCreatedAtIsRequired = errors.New("created_at is required")
)
