package services

import (
	"context"
	"time"

	"github.com/fastprodman/consistent-store/internal/domains/notification/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/notification/ports/in"
	portsout "github.com/fastprodman/consistent-store/internal/domains/notification/ports/out"
	"github.com/google/uuid"
)

type Service struct {
	tx       portsout.TransactionManager
	notifier portsout.Notifier
	events   portsout.EventPublisher
}

var _ portsin.OrderCreatedHandler = (*Service)(nil)
var _ portsin.CustomerCreatedHandler = (*Service)(nil)

func NewService(
	tx portsout.TransactionManager,
	notifier portsout.Notifier,
	events portsout.EventPublisher,
) *Service {
	return &Service{
		tx:       tx,
		notifier: notifier,
		events:   events,
	}
}

func (s *Service) HandleOrderCreated(
	ctx context.Context,
	event entities.OrderCreatedEvent,
) (notified bool, err error) {
	err = s.tx.Exec(ctx, func(ctx context.Context) error {
		var err error

		notified, err = s.notifier.NotifyOrderCreated(ctx, event)
		if err != nil {
			return err
		}

		if !notified {
			return nil
		}

		notificationSentEvent, err := entities.NewOrderNotificationSentEvent(
			uuid.New(),
			event.OrderID(),
			event.CustomerID(),
			time.Now().UTC(),
		)
		if err != nil {
			return err
		}

		return s.events.PublishOrderNotificationSent(ctx, notificationSentEvent)
	})
	if err != nil {
		return false, err
	}

	return notified, nil
}

func (s *Service) HandleCustomerCreated(
	ctx context.Context,
	event entities.CustomerCreatedEvent,
) error {
	return s.notifier.NotifyCustomerCreated(ctx, event)
}
