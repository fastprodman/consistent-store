package sqltx

import (
	"context"
	"database/sql"
	"errors"
)

type contextKey struct{}

type Store struct {
	db   *sql.DB
	opts *sql.TxOptions
	key  contextKey
}

func New(db *sql.DB, opts *sql.TxOptions) *Store {
	return &Store{
		db:   db,
		opts: opts,
	}
}

func (s *Store) Exec(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := s.SQLTx(ctx); ok {
		return fn(ctx)
	}

	tx, err := s.db.BeginTx(ctx, s.opts)
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			_ = rollback(tx)
			panic(r)
		}
	}()

	ctx = context.WithValue(ctx, s.key, tx)

	if err := fn(ctx); err != nil {
		if rbErr := rollback(tx); rbErr != nil {
			return errors.Join(err, rbErr)
		}

		return err
	}

	return tx.Commit()
}

func (s *Store) SQLTx(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(s.key).(*sql.Tx)
	return tx, ok
}

func rollback(tx *sql.Tx) error {
	err := tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}

	return err
}
