package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"

	notifierin "github.com/fastprodman/consistent-store/internal/domains/ordernotifier/adapters/in"
	notifierout "github.com/fastprodman/consistent-store/internal/domains/ordernotifier/adapters/out"
	notifierservices "github.com/fastprodman/consistent-store/internal/domains/ordernotifier/services"
	"github.com/fastprodman/consistent-store/internal/shared/db"
	"github.com/fastprodman/consistent-store/pkg/sqltx"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := getenv("DATABASE_URL", "postgres://app:app@localhost:5432/outbox_demo?sslmode=disable")
	broker := getenv("KAFKA_BROKER", "localhost:19092")

	database, err := db.OpenPostgres(ctx, dsn)
	if err != nil {
		log.Fatal("open postgres:", err)
	}
	defer database.Close()

	txStore := sqltx.New(database, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})

	provider := db.NewProvider(database, txStore)
	notifier := notifierout.NewNotifier(provider)
	handler := notifierservices.NewOrderCreatedHandler(notifier)
	consumer := notifierin.NewKafkaConsumer(broker, handler)
	defer consumer.Close()

	consumer.Run(ctx)
}

func getenv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
