package services

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/notification/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/notification/ports/in"
	portsout "github.com/fastprodman/consistent-store/internal/domains/notification/ports/out"
)

type Service struct {
	notifier portsout.Notifier
}

var _ portsin.OrderCreatedHandler = (*Service)(nil)
var _ portsin.CustomerCreatedHandler = (*Service)(nil)

func NewService(notifier portsout.Notifier) *Service {
	return &Service{
		notifier: notifier,
	}
}

func (s *Service) HandleOrderCreated(
	ctx context.Context,
	event entities.OrderCreatedEvent,
) (notified bool, err error) {
	return s.notifier.NotifyOrderCreated(ctx, event)
}

func (s *Service) HandleCustomerCreated(
	ctx context.Context,
	event entities.CustomerCreatedEvent,
) error {
	return s.notifier.NotifyCustomerCreated(ctx, event)
}
