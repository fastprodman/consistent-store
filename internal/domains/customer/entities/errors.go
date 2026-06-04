package entities

import "errors"

var (
	ErrEmptyCustomerID = errors.New("customer_id is required")
	ErrEmptyEventID    = errors.New("event_id is required")
	ErrEmptyCreatedAt  = errors.New("created_at is required")
)
