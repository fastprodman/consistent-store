package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/fastprodman/consistent-store/internal/db"
	"github.com/fastprodman/consistent-store/internal/httpapi"
	"github.com/fastprodman/consistent-store/internal/inventory"
	"github.com/fastprodman/consistent-store/internal/orders"
	"github.com/fastprodman/consistent-store/internal/outbox"
	"github.com/fastprodman/consistent-store/pkg/sqltx"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://app:app@localhost:5432/outbox_demo?sslmode=disable"
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

	orderHandler := httpapi.NewOrderHandler(orderService, orderRepo)
	inventoryHandler := httpapi.NewInventoryHandler(inventoryRepo)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /orders", orderHandler.CreateOrder)
	mux.HandleFunc("GET /orders/", orderHandler.GetOrder)
	mux.HandleFunc("GET /inventory/", inventoryHandler.GetInventory)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Println("order service listening on :8080")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
