package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/fastprodman/consistent-store/internal/db"
	"github.com/fastprodman/consistent-store/internal/inventory"
	"github.com/fastprodman/consistent-store/internal/orders"
	"github.com/fastprodman/consistent-store/internal/outbox"
	"github.com/fastprodman/consistent-store/pkg/sqltx"
	"github.com/google/uuid"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://app:app@localhost:5432/my_store?sslmode=disable"
	}

	database, err := db.OpenPostgres(ctx, dsn)
	if err != nil {
		log.Fatal("open postgres:", err)
	}
	defer database.Close()

	txStore := sqltx.New(database, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})

	provider := db.NewProvider(database, txStore)

	inventoryRepo := inventory.NewRepository(provider)
	orderRepo := orders.NewRepository(provider)
	outboxRepo := outbox.NewRepository(provider)

	orderService := orders.NewService(
		txStore,
		orderRepo,
		inventoryRepo,
		outboxRepo,
	)

	fmt.Println("=== BEFORE ===")
	printInventory(ctx, inventoryRepo, "BOOK-001")

	fmt.Println()
	fmt.Println("=== CREATE ORDER ===")

	orderID, err := orderService.CreateOrder(ctx, orders.CreateOrderCommand{
		CustomerID: "cust_123",
		Items: []orders.CreateOrderItem{
			{
				SKU:      "BOOK-001",
				Quantity: 2,
			},
		},
	})
	if err != nil {
		log.Fatal("create order:", err)
	}

	fmt.Println("created order:", orderID)

	fmt.Println()
	fmt.Println("=== AFTER SUCCESS ===")
	printInventory(ctx, inventoryRepo, "BOOK-001")
	printOrder(ctx, orderRepo, orderID)
	printOutboxEvents(ctx, database)

	fmt.Println()
	fmt.Println("=== FORCE ROLLBACK TEST ===")

	beforeRollback, err := inventoryRepo.Get(ctx, "BOOK-001")
	if err != nil {
		log.Fatal("get inventory before rollback:", err)
	}

	err = txStore.Exec(ctx, func(ctx context.Context) error {
		_, err := inventoryRepo.Reserve(ctx, "BOOK-001", 1)
		if err != nil {
			return err
		}

		return errors.New("forced rollback")
	})
	if err == nil {
		log.Fatal("expected forced rollback error, got nil")
	}

	fmt.Println("rollback error:", err)

	afterRollback, err := inventoryRepo.Get(ctx, "BOOK-001")
	if err != nil {
		log.Fatal("get inventory after rollback:", err)
	}

	fmt.Printf("inventory before rollback test: available=%d\n", beforeRollback.AvailableQuantity)
	fmt.Printf("inventory after rollback test:  available=%d\n", afterRollback.AvailableQuantity)

	if beforeRollback.AvailableQuantity == afterRollback.AvailableQuantity {
		fmt.Println("rollback worked: inventory unchanged")
	} else {
		fmt.Println("rollback failed: inventory changed")
	}

	fmt.Println()
	fmt.Println("=== INSUFFICIENT INVENTORY TEST ===")

	beforeInsufficient, err := inventoryRepo.Get(ctx, "BOOK-001")
	if err != nil {
		log.Fatal("get inventory before insufficient test:", err)
	}

	err = txStore.Exec(ctx, func(ctx context.Context) error {
		_, err := inventoryRepo.Reserve(ctx, "BOOK-001", 999999)
		return err
	})
	if err != nil {
		fmt.Println("expected error:", err)
	} else {
		fmt.Println("unexpected success")
	}

	afterInsufficient, err := inventoryRepo.Get(ctx, "BOOK-001")
	if err != nil {
		log.Fatal("get inventory after insufficient test:", err)
	}

	fmt.Printf("inventory before insufficient test: available=%d\n", beforeInsufficient.AvailableQuantity)
	fmt.Printf("inventory after insufficient test:  available=%d\n", afterInsufficient.AvailableQuantity)

	if beforeInsufficient.AvailableQuantity == afterInsufficient.AvailableQuantity {
		fmt.Println("insufficient inventory rollback worked: inventory unchanged")
	} else {
		fmt.Println("insufficient inventory test failed: inventory changed")
	}
}

func printInventory(ctx context.Context, repo *inventory.Repository, sku string) {
	item, err := repo.Get(ctx, sku)
	if err != nil {
		log.Fatal("get inventory:", err)
	}

	fmt.Printf("inventory %s: available=%d price_cents=%d\n",
		item.SKU,
		item.AvailableQuantity,
		item.PriceCents,
	)
}

func printOrder(ctx context.Context, repo *orders.Repository, id uuid.UUID) {
	order, err := repo.Get(ctx, id)
	if err != nil {
		log.Fatal("get order:", err)
	}

	fmt.Printf("order: id=%s customer_id=%s status=%s total_cents=%d\n",
		order.ID,
		order.CustomerID,
		order.Status,
		order.TotalCents,
	)
}

func printOutboxEvents(ctx context.Context, database *sql.DB) {
	rows, err := database.QueryContext(ctx, `
		SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
		FROM outbox_events
		ORDER BY created_at DESC
		LIMIT 5
	`)
	if err != nil {
		log.Fatal("query outbox events:", err)
	}
	defer rows.Close()

	fmt.Println("latest outbox events:")

	for rows.Next() {
		var (
			id            string
			aggregateType string
			aggregateID   string
			eventType     string
			payload       string
			createdAt     time.Time
		)

		if err := rows.Scan(
			&id,
			&aggregateType,
			&aggregateID,
			&eventType,
			&payload,
			&createdAt,
		); err != nil {
			log.Fatal("scan outbox event:", err)
		}

		fmt.Printf("- id=%s aggregate_type=%s aggregate_id=%s event_type=%s created_at=%s\n",
			id,
			aggregateType,
			aggregateID,
			eventType,
			createdAt.Format(time.RFC3339),
		)
		fmt.Println("  payload:", payload)
	}

	if err := rows.Err(); err != nil {
		log.Fatal("outbox rows:", err)
	}
}
