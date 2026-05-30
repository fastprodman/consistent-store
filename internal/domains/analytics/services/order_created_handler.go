package services

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/analytics/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/analytics/ports/in"
	portsout "github.com/fastprodman/consistent-store/internal/domains/analytics/ports/out"
)

type OrderCreatedHandler struct {
	projector portsout.Projector
}

var _ portsin.OrderCreatedHandler = (*OrderCreatedHandler)(nil)

func NewOrderCreatedHandler(projector portsout.Projector) *OrderCreatedHandler {
	return &OrderCreatedHandler{
		projector: projector,
	}
}

func (h *OrderCreatedHandler) HandleOrderCreated(
	ctx context.Context,
	event entities.OrderCreatedEvent,
) (applied bool, err error) {
	return h.projector.ApplyOrderCreated(ctx, event)
}
