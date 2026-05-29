package db

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/lib/pq"
)

func OpenPostgres(ctx context.Context, dsn string) (*sql.DB, error) {
	database, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	database.SetMaxOpenConns(10)
	database.SetMaxIdleConns(10)
	database.SetConnMaxLifetime(30 * time.Minute)

	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, err
	}

	return database, nil
}
