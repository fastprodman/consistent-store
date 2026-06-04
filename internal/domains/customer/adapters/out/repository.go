package out

import (
	"context"
	"database/sql"
	"errors"

	"github.com/fastprodman/consistent-store/internal/domains/customer/entities"
	portsout "github.com/fastprodman/consistent-store/internal/domains/customer/ports/out"
	"github.com/fastprodman/consistent-store/internal/shared/db"
)

type Repository struct {
	db *db.Provider
}

var _ portsout.Repository = (*Repository)(nil)

func NewRepository(db *db.Provider) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Create(ctx context.Context, customer entities.Customer) (created bool, err error) {
	exec := r.db.Executor(ctx)

	var didInsert bool

	err = exec.QueryRowContext(ctx, `
		INSERT INTO customers (id)
		VALUES ($1)
		ON CONFLICT (id) DO NOTHING
		RETURNING true
	`, customer.ID()).Scan(&didInsert)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return didInsert, nil
}
