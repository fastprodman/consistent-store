package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	customerhttp "github.com/fastprodman/consistent-store/internal/domains/customer/adapters/in"
	customerout "github.com/fastprodman/consistent-store/internal/domains/customer/adapters/out"
	customerservices "github.com/fastprodman/consistent-store/internal/domains/customer/services"
	inventoryhttp "github.com/fastprodman/consistent-store/internal/domains/inventory/adapters/in"
	inventoryout "github.com/fastprodman/consistent-store/internal/domains/inventory/adapters/out"
	inventoryservices "github.com/fastprodman/consistent-store/internal/domains/inventory/services"
	orderhttp "github.com/fastprodman/consistent-store/internal/domains/order/adapters/in"
	orderout "github.com/fastprodman/consistent-store/internal/domains/order/adapters/out"
	orderservices "github.com/fastprodman/consistent-store/internal/domains/order/services"
	"github.com/fastprodman/consistent-store/internal/shared/db"
	"github.com/fastprodman/consistent-store/internal/shared/observability"
	"github.com/fastprodman/consistent-store/pkg/sqltx"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	shutdownTracing, err := observability.InitTracing(ctx, "order-service")
	if err != nil {
		log.Fatal("init tracing:", err)
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			log.Println("shutdown tracing:", err)
		}
	}()

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
	customerRepo := customerout.NewRepository(provider)
	customerEventPublisher := customerout.NewOutboxEventPublisher(provider)
	customerService := customerservices.NewService(
		txStore,
		customerRepo,
		customerEventPublisher,
	)

	orderService := orderservices.NewService(
		txStore,
		orderRepo,
		inventoryReservationService,
		orderEventPublisher,
	)

	orderHandler := orderhttp.NewHTTPHandler(orderService, orderRepo)
	inventoryHandler := inventoryhttp.NewHTTPHandler(inventoryQueryService)
	customerHandler := customerhttp.NewHTTPHandler(customerService)

	mux := http.NewServeMux()

	mux.Handle("POST /customers", otelhttp.NewHandler(
		http.HandlerFunc(customerHandler.CreateCustomer),
		"POST /customers",
	))
	mux.Handle("POST /orders", otelhttp.NewHandler(
		http.HandlerFunc(orderHandler.CreateOrder),
		"POST /orders",
	))
	mux.Handle("GET /orders/", otelhttp.NewHandler(
		http.HandlerFunc(orderHandler.GetOrder),
		"GET /orders/{id}",
	))
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
