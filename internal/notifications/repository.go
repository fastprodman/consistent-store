package notifications

import (
	"context"
	"database/sql"
	"errors"

	"github.com/fastprodman/consistent-store/internal/db"
	"github.com/google/uuid"
)

type Repository struct {
	db *db.Provider
}

func NewRepository(db *db.Provider) *Repository {
	return &Repository{db: db}
}

func (r *Repository) LogProcessed(
	ctx context.Context,
	eventID uuid.UUID,
	orderID uuid.UUID,
	customerID string,
) (inserted bool, err error) {
	exec := r.db.Executor(ctx)

	var didInsert bool

	err = exec.QueryRowContext(ctx, `
		INSERT INTO notification_log (
			event_id,
			order_id,
			customer_id,
			status
		)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING true
	`, eventID, orderID, customerID, "sent").Scan(&didInsert)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return didInsert, nil
}
