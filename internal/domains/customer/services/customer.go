package services

import (
	"context"
	"time"

	"github.com/fastprodman/consistent-store/internal/domains/customer/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/customer/ports/in"
	portsout "github.com/fastprodman/consistent-store/internal/domains/customer/ports/out"
	"github.com/google/uuid"
)

type Service struct {
	tx        portsout.TransactionManager
	customers portsout.Repository
	events    portsout.EventPublisher
}

var _ portsin.Service = (*Service)(nil)

func NewService(
	tx portsout.TransactionManager,
	customers portsout.Repository,
	events portsout.EventPublisher,
) *Service {
	return &Service{
		tx:        tx,
		customers: customers,
		events:    events,
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

	err = s.tx.Exec(ctx, func(ctx context.Context) error {
		created, err := s.customers.Create(ctx, customer)
		if err != nil {
			return err
		}

		if !created {
			return ErrCustomerAlreadyExists
		}

		event, err := entities.NewCustomerCreatedEvent(
			uuid.New(),
			customer.ID(),
			time.Now().UTC(),
		)
		if err != nil {
			return err
		}

		return s.events.PublishCustomerCreated(ctx, event)
	})
	if err != nil {
		return "", err
	}

	return customer.ID(), nil
}
