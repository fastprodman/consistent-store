package inventory

import (
	"context"
	"database/sql"
	"errors"

	"github.com/fastprodman/consistent-store/internal/db"
)

type Repository struct {
	db *db.Provider
}

func NewRepository(db *db.Provider) *Repository {
	return &Repository{
		db: db,
	}
}

func (r *Repository) Reserve(ctx context.Context, sku string, quantity int) (priceCents int, err error) {
	if quantity <= 0 {
		return 0, ErrInvalidQuantity
	}

	exec := r.db.Executor(ctx)

	row := exec.QueryRowContext(ctx, `
		UPDATE inventory
		SET available_quantity = available_quantity - $1,
		    updated_at = now()
		WHERE sku = $2
		  AND available_quantity >= $1
		RETURNING price_cents
	`, quantity, sku)

	if err := row.Scan(&priceCents); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrInsufficientInventory
		}

		return 0, err
	}

	return priceCents, nil
}

func (r *Repository) Get(ctx context.Context, sku string) (Item, error) {
	exec := r.db.Executor(ctx)

	var item Item

	err := exec.QueryRowContext(ctx, `
		SELECT sku, available_quantity, price_cents
		FROM inventory
		WHERE sku = $1
	`, sku).Scan(
		&item.SKU,
		&item.AvailableQuantity,
		&item.PriceCents,
	)

	if err != nil {
		return Item{}, err
	}

	return item, nil
}
