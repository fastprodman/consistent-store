package sqltx

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/google/uuid"
)

type Tx struct {
	Code string
	Tx   *sql.Tx
}

type Store struct {
	db *sql.DB

	opts *sql.TxOptions
	key  *struct{}

	mu           sync.RWMutex
	transactions map[string]*sql.Tx
}

func New(db *sql.DB, opts *sql.TxOptions) *Store {
	return &Store{
		db:           db,
		opts:         opts,
		key:          &struct{}{},
		transactions: make(map[string]*sql.Tx),
	}
}

func (s *Store) Exec(
	ctx context.Context,
	fn func(ctx context.Context) error,
) error {
	if _, ok := s.FromContext(ctx); ok {
		return fn(ctx)
	}

	sqlTx, err := s.db.BeginTx(ctx, s.opts)
	if err != nil {
		return err
	}

	tx := Tx{
		Code: uuid.NewString(),
		Tx:   sqlTx,
	}

	s.mu.Lock()
	s.transactions[tx.Code] = tx.Tx
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.transactions, tx.Code)
		s.mu.Unlock()
	}()

	defer func() {
		if r := recover(); r != nil {
			_ = rollback(tx.Tx)
			panic(r)
		}
	}()

	ctx = context.WithValue(ctx, s.key, tx)

	if err := fn(ctx); err != nil {
		if rbErr := rollback(tx.Tx); rbErr != nil {
			return errors.Join(err, rbErr)
		}

		return err
	}

	if err := tx.Tx.Commit(); err != nil {
		_ = rollback(tx.Tx)
		return err
	}

	return nil
}

func (s *Store) FromContext(ctx context.Context) (Tx, bool) {
	tx, ok := ctx.Value(s.key).(Tx)
	return tx, ok
}

func (s *Store) SQLTx(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := s.FromContext(ctx)
	if !ok {
		return nil, false
	}

	return tx.Tx, true
}

func (s *Store) Get(code string) (*sql.Tx, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tx, ok := s.transactions[code]
	return tx, ok
}

func rollback(tx *sql.Tx) error {
	err := tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}

	return err
}
