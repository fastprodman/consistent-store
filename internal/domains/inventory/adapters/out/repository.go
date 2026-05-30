package out

import (
	"context"
	"database/sql"
	"errors"

	"github.com/fastprodman/consistent-store/internal/db"
	"github.com/fastprodman/consistent-store/internal/domains/inventory/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/inventory/ports/out"
)

type Repository struct {
	db *db.Provider
}

var _ portsout.Repository = (*Repository)(nil)

func NewRepository(db *db.Provider) *Repository {
	return &Repository{
		db: db,
	}
}

func (r *Repository) Reserve(ctx context.Context, sku string, quantity int) (priceCents int, err error) {
	if quantity <= 0 {
		return 0, entities.ErrInvalidQuantity
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
			return 0, entities.ErrInsufficientInventory
		}

		return 0, err
	}

	return priceCents, nil
}

func (r *Repository) Get(ctx context.Context, sku string) (entities.Item, error) {
	exec := r.db.Executor(ctx)

	var availableQuantity int
	var priceCents int

	err := exec.QueryRowContext(ctx, `
		SELECT available_quantity, price_cents
		FROM inventory
		WHERE sku = $1
	`, sku).Scan(
		&availableQuantity,
		&priceCents,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return entities.Item{}, entities.ErrNotFound
		}

		return entities.Item{}, err
	}

	return entities.NewItem(sku, availableQuantity, priceCents), nil
}
