package services

import "errors"

var (
	ErrEmptyCustomerID = errors.New("customer_id is required")
	ErrEmptyItems      = errors.New("order must contain at least one item")
)
