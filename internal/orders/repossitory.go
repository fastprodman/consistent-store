package orders

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/db"
	"github.com/google/uuid"
)

type Repository struct {
	db *db.Provider
}

func NewRepository(db *db.Provider) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Create(ctx context.Context, order Order) error {
	exec := r.db.Executor(ctx)

	_, err := exec.ExecContext(ctx, `
		INSERT INTO orders (id, customer_id, status, total_cents)
		VALUES ($1, $2, $3, $4)
	`, order.ID, order.CustomerID, order.Status, order.TotalCents)

	return err
}

func (r *Repository) CreateItem(ctx context.Context, item OrderItem) error {
	exec := r.db.Executor(ctx)

	_, err := exec.ExecContext(ctx, `
		INSERT INTO order_items (id, order_id, sku, quantity, price_cents)
		VALUES ($1, $2, $3, $4, $5)
	`, item.ID, item.OrderID, item.SKU, item.Quantity, item.PriceCents)

	return err
}

func (r *Repository) Get(ctx context.Context, id uuid.UUID) (Order, error) {
	exec := r.db.Executor(ctx)

	var order Order

	err := exec.QueryRowContext(ctx, `
		SELECT id, customer_id, status, total_cents
		FROM orders
		WHERE id = $1
	`, id).Scan(
		&order.ID,
		&order.CustomerID,
		&order.Status,
		&order.TotalCents,
	)

	return order, err
}

func (r *Repository) GetItems(ctx context.Context, orderID uuid.UUID) ([]OrderItem, error) {
	exec := r.db.Executor(ctx)

	rows, err := exec.QueryContext(ctx, `
		SELECT id, order_id, sku, quantity, price_cents
		FROM order_items
		WHERE order_id = $1
		ORDER BY sku
	`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []OrderItem

	for rows.Next() {
		var item OrderItem

		if err := rows.Scan(
			&item.ID,
			&item.OrderID,
			&item.SKU,
			&item.Quantity,
			&item.PriceCents,
		); err != nil {
			return nil, err
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}
