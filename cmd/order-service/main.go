package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/fastprodman/consistent-store/internal/db"
	inventoryhttp "github.com/fastprodman/consistent-store/internal/domains/inventory/adapters/in"
	inventoryout "github.com/fastprodman/consistent-store/internal/domains/inventory/adapters/out"
	inventoryservices "github.com/fastprodman/consistent-store/internal/domains/inventory/services"
	orderhttp "github.com/fastprodman/consistent-store/internal/domains/order/adapters/in"
	orderout "github.com/fastprodman/consistent-store/internal/domains/order/adapters/out"
	orderservices "github.com/fastprodman/consistent-store/internal/domains/order/services"
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

	domainInventoryRepo := inventoryout.NewRepository(provider)
	inventoryQueryService := inventoryservices.NewQueryService(domainInventoryRepo)
	inventoryReservationService := inventoryservices.NewReservationService(domainInventoryRepo)
	orderRepo := orderout.NewRepository(provider)
	orderEventPublisher := orderout.NewOutboxEventPublisher(provider)

	orderService := orderservices.NewService(
		txStore,
		orderRepo,
		inventoryReservationService,
		orderEventPublisher,
	)

	orderHandler := orderhttp.NewHTTPHandler(orderService, orderRepo)
	inventoryHandler := inventoryhttp.NewHTTPHandler(inventoryQueryService)

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
