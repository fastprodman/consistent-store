package services

import (
	"context"

	portsout "github.com/fastprodman/consistent-store/internal/domains/inventory/ports/out"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type ReservationService struct {
	inventory portsout.Repository
}

func NewReservationService(inventory portsout.Repository) *ReservationService {
	return &ReservationService{
		inventory: inventory,
	}
}

func (s *ReservationService) Reserve(ctx context.Context, sku string, quantity int) (priceCents int, err error) {
	ctx, span := otel.Tracer("inventory.service").Start(
		ctx,
		"inventory.reserve",
		trace.WithAttributes(
			attribute.String("inventory.sku", sku),
			attribute.Int("inventory.quantity", quantity),
		),
	)
	defer span.End()

	priceCents, err = s.inventory.Reserve(ctx, sku, quantity)
	if err != nil {
		recordSpanError(span, err)
		return 0, err
	}

	span.SetAttributes(attribute.Int("inventory.price_cents", priceCents))

	return priceCents, nil
}

func recordSpanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
