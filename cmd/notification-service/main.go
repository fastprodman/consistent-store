package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"

	notificationin "github.com/fastprodman/consistent-store/internal/domains/notification/adapters/in"
	notificationout "github.com/fastprodman/consistent-store/internal/domains/notification/adapters/out"
	notificationservices "github.com/fastprodman/consistent-store/internal/domains/notification/services"
	"github.com/fastprodman/consistent-store/internal/shared/db"
	"github.com/fastprodman/consistent-store/internal/shared/observability"
	"github.com/fastprodman/consistent-store/pkg/sqltx"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.InitTracing(ctx, "notification-service")
	if err != nil {
		log.Fatal("init tracing:", err)
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			log.Println("shutdown tracing:", err)
		}
	}()

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
	notifier := notificationout.NewNotifier(provider)
	eventPublisher := notificationout.NewOutboxEventPublisher(provider)
	handler := notificationservices.NewService(txStore, notifier, eventPublisher)
	consumer := notificationin.NewKafkaConsumer(broker, handler)
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
