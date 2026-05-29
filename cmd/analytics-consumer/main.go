package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fastprodman/consistent-store/internal/analytics"
	"github.com/fastprodman/consistent-store/internal/db"
	"github.com/fastprodman/consistent-store/internal/events"
	"github.com/fastprodman/consistent-store/pkg/sqltx"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
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
	analyticsRepo := analytics.NewRepository(provider)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          "order.events",
		GroupID:        "analytics-consumer",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()

	log.Println("analytics consumer started")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("analytics consumer stopped")
				return
			}

			log.Println("fetch message:", err)
			continue
		}

		if err := handleMessage(ctx, txStore, analyticsRepo, msg); err != nil {
			log.Println("handle message:", err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Println("commit message:", err)
			continue
		}
	}
}

func handleMessage(
	ctx context.Context,
	txStore *sqltx.Store,
	analyticsRepo *analytics.Repository,
	msg kafka.Message,
) error {
	var event events.OrderCreated

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}

	if event.EventID == uuid.Nil {
		return fmt.Errorf("event_id is required")
	}

	if event.OrderID == uuid.Nil {
		return fmt.Errorf("order_id is required")
	}

	if event.TotalCents <= 0 {
		return fmt.Errorf("total_cents must be positive")
	}

	return txStore.Exec(ctx, func(ctx context.Context) error {
		inserted, err := analyticsRepo.ApplyOrderCreated(
			ctx,
			event.EventID,
			event.CreatedAt,
			event.TotalCents,
		)
		if err != nil {
			return err
		}

		if inserted {
			log.Printf(
				"analytics updated: topic=%s partition=%d offset=%d event_id=%s total_cents=%d",
				msg.Topic,
				msg.Partition,
				msg.Offset,
				event.EventID,
				event.TotalCents,
			)
		} else {
			log.Printf("duplicate analytics event ignored: event_id=%s", event.EventID)
		}

		return nil
	})
}

func getenv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
