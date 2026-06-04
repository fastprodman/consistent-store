package services

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/customer/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/customer/ports/in"
	portsout "github.com/fastprodman/consistent-store/internal/domains/customer/ports/out"
	"github.com/google/uuid"
)

type Service struct {
	customers portsout.Repository
}

var _ portsin.Service = (*Service)(nil)

func NewService(customers portsout.Repository) *Service {
	return &Service{
		customers: customers,
	}
}

func (s *Service) CreateCustomer(ctx context.Context, cmd portsin.CreateCustomerCommand) (string, error) {
	customerID := cmd.CustomerID
	if customerID == "" {
		customerID = uuid.NewString()
	}

	customer, err := entities.NewCustomer(customerID)
	if err != nil {
		return "", err
	}

	created, err := s.customers.Create(ctx, customer)
	if err != nil {
		return "", err
	}

	if !created {
		return "", ErrCustomerAlreadyExists
	}

	return customer.ID(), nil
}
