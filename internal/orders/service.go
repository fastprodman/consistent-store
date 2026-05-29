package orders

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/fastprodman/consistent-store/internal/events"
	"github.com/fastprodman/consistent-store/internal/inventory"
	"github.com/fastprodman/consistent-store/internal/outbox"
	"github.com/fastprodman/consistent-store/pkg/sqltx"
	"github.com/google/uuid"
)

var (
	ErrEmptyCustomerID = errors.New("customer_id is required")
	ErrEmptyItems      = errors.New("order must contain at least one item")
)

type Service struct {
	tx        *sqltx.Store
	orders    *Repository
	inventory *inventory.Repository
	outbox    *outbox.Repository
}

func NewService(
	tx *sqltx.Store,
	orders *Repository,
	inventory *inventory.Repository,
	outbox *outbox.Repository,
) *Service {
	return &Service{
		tx:        tx,
		orders:    orders,
		inventory: inventory,
		outbox:    outbox,
	}
}

func (s *Service) CreateOrder(ctx context.Context, cmd CreateOrderCommand) (uuid.UUID, error) {
	if cmd.CustomerID == "" {
		return uuid.Nil, ErrEmptyCustomerID
	}

	if len(cmd.Items) == 0 {
		return uuid.Nil, ErrEmptyItems
	}

	orderID := uuid.New()

	err := s.tx.Exec(ctx, func(ctx context.Context) error {
		var totalCents int
		orderItems := make([]OrderItem, 0, len(cmd.Items))
		eventItems := make([]events.OrderCreatedItem, 0, len(cmd.Items))

		for _, item := range cmd.Items {
			priceCents, err := s.inventory.Reserve(ctx, item.SKU, item.Quantity)
			if err != nil {
				return err
			}

			totalCents += priceCents * item.Quantity

			orderItems = append(orderItems, OrderItem{
				ID:         uuid.New(),
				OrderID:    orderID,
				SKU:        item.SKU,
				Quantity:   item.Quantity,
				PriceCents: priceCents,
			})

			eventItems = append(eventItems, events.OrderCreatedItem{
				SKU:        item.SKU,
				Quantity:   item.Quantity,
				PriceCents: priceCents,
			})
		}

		order := Order{
			ID:         orderID,
			CustomerID: cmd.CustomerID,
			Status:     "created",
			TotalCents: totalCents,
		}

		if err := s.orders.Create(ctx, order); err != nil {
			return err
		}

		for _, item := range orderItems {
			if err := s.orders.CreateItem(ctx, item); err != nil {
				return err
			}
		}

		eventID := uuid.New()

		payload, err := json.Marshal(events.OrderCreated{
			EventID:    eventID,
			EventType:  "OrderCreated",
			OrderID:    orderID,
			CustomerID: cmd.CustomerID,
			Items:      eventItems,
			TotalCents: totalCents,
			CreatedAt:  time.Now().UTC(),
		})
		if err != nil {
			return err
		}

		return s.outbox.Add(ctx, outbox.Event{
			ID:            eventID,
			AggregateType: "order",
			AggregateID:   orderID,
			EventType:     "OrderCreated",
			Payload:       payload,
		})
	})

	if err != nil {
		return uuid.Nil, err
	}

	return orderID, nil
}
