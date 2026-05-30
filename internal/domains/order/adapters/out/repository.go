package out

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/order/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/order/ports/out"
	"github.com/fastprodman/consistent-store/internal/shared/db"
	"github.com/google/uuid"
)

type Repository struct {
	db *db.Provider
}

var _ portsout.Repository = (*Repository)(nil)

func NewRepository(db *db.Provider) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Create(ctx context.Context, order entities.Order) error {
	exec := r.db.Executor(ctx)

	_, err := exec.ExecContext(ctx, `
		INSERT INTO orders (id, customer_id, status, total_cents)
		VALUES ($1, $2, $3, $4)
	`, order.ID(), order.CustomerID(), string(order.Status()), order.TotalCents())

	return err
}

func (r *Repository) CreateItem(ctx context.Context, item entities.OrderItem) error {
	exec := r.db.Executor(ctx)

	_, err := exec.ExecContext(ctx, `
		INSERT INTO order_items (id, order_id, sku, quantity, price_cents)
		VALUES ($1, $2, $3, $4, $5)
	`, item.ID(), item.OrderID(), item.SKU(), item.Quantity(), item.PriceCents())

	return err
}

func (r *Repository) Get(ctx context.Context, id uuid.UUID) (entities.Order, error) {
	exec := r.db.Executor(ctx)

	var customerID string
	var status string
	var totalCents int

	err := exec.QueryRowContext(ctx, `
		SELECT customer_id, status, total_cents
		FROM orders
		WHERE id = $1
	`, id).Scan(
		&customerID,
		&status,
		&totalCents,
	)
	if err != nil {
		return entities.Order{}, err
	}

	return entities.NewOrder(id, customerID, entities.OrderStatus(status), totalCents), nil
}

func (r *Repository) GetItems(ctx context.Context, orderID uuid.UUID) ([]entities.OrderItem, error) {
	exec := r.db.Executor(ctx)

	rows, err := exec.QueryContext(ctx, `
		SELECT id, sku, quantity, price_cents
		FROM order_items
		WHERE order_id = $1
		ORDER BY sku
	`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []entities.OrderItem

	for rows.Next() {
		var id uuid.UUID
		var sku string
		var quantity int
		var priceCents int

		if err := rows.Scan(
			&id,
			&sku,
			&quantity,
			&priceCents,
		); err != nil {
			return nil, err
		}

		items = append(items, entities.NewOrderItem(
			id,
			orderID,
			sku,
			quantity,
			priceCents,
		))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}
