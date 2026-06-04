package out

import "context"

type TransactionManager interface {
	Exec(ctx context.Context, fn func(ctx context.Context) error) error
}
