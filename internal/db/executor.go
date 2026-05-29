package db

import (
	"context"
	"database/sql"

	"github.com/fastprodman/consistent-store/pkg/sqltx"
)

type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Provider struct {
	db *sql.DB
	tx *sqltx.Store
}

func NewProvider(db *sql.DB, tx *sqltx.Store) *Provider {
	return &Provider{
		db: db,
		tx: tx,
	}
}

func (p *Provider) Executor(ctx context.Context) Executor {
	if tx, ok := p.tx.SQLTx(ctx); ok {
		return tx
	}

	return p.db
}
