package in

import "context"

type Service interface {
	CreateCustomer(ctx context.Context, cmd CreateCustomerCommand) (string, error)
}

type CreateCustomerCommand struct {
	CustomerID string
}
