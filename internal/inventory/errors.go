package inventory

import "errors"

var (
	ErrInsufficientInventory = errors.New("insufficient inventory")
	ErrInvalidQuantity       = errors.New("quantity must be greater than zero")
	ErrNotFound              = errors.New("inventory item not found")
)
