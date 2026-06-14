package services

import (
	"context"
	"time"

	"github.com/fastprodman/consistent-store/internal/domains/order/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/order/ports/in"
	portsout "github.com/fastprodman/consistent-store/internal/domains/order/ports/out"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Service struct {
	tx        portsout.TransactionManager
	orders    portsout.Repository
	inventory portsout.Inventory
	events    portsout.EventPublisher
}

var _ portsin.Service = (*Service)(nil)

func NewService(
	tx portsout.TransactionManager,
	orders portsout.Repository,
	inventory portsout.Inventory,
	events portsout.EventPublisher,
) *Service {
	return &Service{
		tx:        tx,
		orders:    orders,
		inventory: inventory,
		events:    events,
	}
}

func (s *Service) CreateOrder(ctx context.Context, cmd portsin.CreateOrderCommand) (uuid.UUID, error) {
	ctx, span := otel.Tracer("order.service").Start(
		ctx,
		"order.create",
		trace.WithAttributes(
			attribute.String("customer.id", cmd.CustomerID),
			attribute.Int("order.item_count", len(cmd.Items)),
		),
	)
	defer span.End()

	if cmd.CustomerID == "" {
		recordSpanError(span, ErrEmptyCustomerID)
		return uuid.Nil, ErrEmptyCustomerID
	}

	if len(cmd.Items) == 0 {
		recordSpanError(span, ErrEmptyItems)
		return uuid.Nil, ErrEmptyItems
	}

	orderID := uuid.New()
	span.SetAttributes(attribute.String("order.id", orderID.String()))

	err := s.tx.Exec(ctx, func(ctx context.Context) error {
		var totalCents int
		orderItems := make([]entities.OrderItem, 0, len(cmd.Items))
		eventItems := make([]entities.OrderCreatedEventItem, 0, len(cmd.Items))

		for _, item := range cmd.Items {
			priceCents, err := s.inventory.Reserve(ctx, item.SKU, item.Quantity)
			if err != nil {
				return err
			}

			totalCents += priceCents * item.Quantity

			orderItems = append(orderItems, entities.NewOrderItem(
				uuid.New(),
				orderID,
				item.SKU,
				item.Quantity,
				priceCents,
			))

			eventItems = append(eventItems, entities.NewOrderCreatedEventItem(
				item.SKU,
				item.Quantity,
				priceCents,
			))
		}

		if err := s.orders.Create(ctx, entities.NewCreatedOrder(
			orderID,
			cmd.CustomerID,
			totalCents,
		)); err != nil {
			return err
		}

		for _, item := range orderItems {
			if err := s.orders.CreateItem(ctx, item); err != nil {
				return err
			}
		}

		return s.events.PublishOrderCreated(ctx, entities.NewOrderCreatedEvent(
			uuid.New(),
			orderID,
			cmd.CustomerID,
			eventItems,
			totalCents,
			time.Now().UTC(),
		))
	})
	if err != nil {
		recordSpanError(span, err)
		return uuid.Nil, err
	}

	return orderID, nil
}

func recordSpanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
