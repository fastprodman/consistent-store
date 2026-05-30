package entities

import "errors"

var (
	ErrEventIDRequired    = errors.New("event_id is required")
	ErrOrderIDRequired    = errors.New("order_id is required")
	ErrCustomerIDRequired = errors.New("customer_id is required")
)
