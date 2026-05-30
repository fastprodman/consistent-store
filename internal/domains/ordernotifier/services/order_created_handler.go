package services

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/ordernotifier/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/ordernotifier/ports/in"
	portsout "github.com/fastprodman/consistent-store/internal/domains/ordernotifier/ports/out"
)

type OrderCreatedHandler struct {
	notifier portsout.Notifier
}

var _ portsin.OrderCreatedHandler = (*OrderCreatedHandler)(nil)

func NewOrderCreatedHandler(notifier portsout.Notifier) *OrderCreatedHandler {
	return &OrderCreatedHandler{
		notifier: notifier,
	}
}

func (h *OrderCreatedHandler) HandleOrderCreated(
	ctx context.Context,
	event entities.OrderCreatedEvent,
) (notified bool, err error) {
	return h.notifier.NotifyOrderCreated(ctx, event)
}
