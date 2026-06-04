package out

import (
	"context"

	"github.com/fastprodman/consistent-store/internal/domains/customer/entities"
)

type Repository interface {
	Create(ctx context.Context, customer entities.Customer) (created bool, err error)
}
