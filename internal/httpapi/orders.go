package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/fastprodman/consistent-store/internal/inventory"
	"github.com/fastprodman/consistent-store/internal/orders"
	"github.com/google/uuid"
)

type OrderHandler struct {
	ordersService *orders.Service
	ordersRepo    *orders.Repository
}

func NewOrderHandler(
	ordersService *orders.Service,
	ordersRepo *orders.Repository,
) *OrderHandler {
	return &OrderHandler{
		ordersService: ordersService,
		ordersRepo:    ordersRepo,
	}
}

type createOrderRequest struct {
	CustomerID string                   `json:"customer_id"`
	Items      []createOrderRequestItem `json:"items"`
}

type createOrderRequestItem struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type createOrderResponse struct {
	OrderID string `json:"order_id"`
}

type orderResponse struct {
	ID         string              `json:"id"`
	CustomerID string              `json:"customer_id"`
	Status     string              `json:"status"`
	TotalCents int                 `json:"total_cents"`
	Items      []orderItemResponse `json:"items"`
}

type orderItemResponse struct {
	SKU        string `json:"sku"`
	Quantity   int    `json:"quantity"`
	PriceCents int    `json:"price_cents"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (h *OrderHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "invalid json body",
		})
		return
	}

	cmd := orders.CreateOrderCommand{
		CustomerID: req.CustomerID,
		Items:      make([]orders.CreateOrderItem, 0, len(req.Items)),
	}

	for _, item := range req.Items {
		cmd.Items = append(cmd.Items, orders.CreateOrderItem{
			SKU:      item.SKU,
			Quantity: item.Quantity,
		})
	}

	orderID, err := h.ordersService.CreateOrder(r.Context(), cmd)
	if err != nil {
		status := http.StatusInternalServerError

		switch {
		case errors.Is(err, orders.ErrEmptyCustomerID),
			errors.Is(err, orders.ErrEmptyItems),
			errors.Is(err, inventory.ErrInvalidQuantity):
			status = http.StatusBadRequest

		case errors.Is(err, inventory.ErrInsufficientInventory):
			status = http.StatusConflict
		}

		writeJSON(w, status, errorResponse{
			Error: err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusCreated, createOrderResponse{
		OrderID: orderID.String(),
	})
}

func (h *OrderHandler) GetOrder(w http.ResponseWriter, r *http.Request) {
	rawID := strings.TrimPrefix(r.URL.Path, "/orders/")
	if rawID == "" || rawID == r.URL.Path {
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: "order not found",
		})
		return
	}

	orderID, err := uuid.Parse(rawID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "invalid order id",
		})
		return
	}

	order, err := h.ordersRepo.Get(r.Context(), orderID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: "order not found",
		})
		return
	}

	items, err := h.ordersRepo.GetItems(r.Context(), orderID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{
			Error: "internal server error",
		})
		return
	}

	resp := orderResponse{
		ID:         order.ID.String(),
		CustomerID: order.CustomerID,
		Status:     order.Status,
		TotalCents: order.TotalCents,
		Items:      make([]orderItemResponse, 0, len(items)),
	}

	for _, item := range items {
		resp.Items = append(resp.Items, orderItemResponse{
			SKU:        item.SKU,
			Quantity:   item.Quantity,
			PriceCents: item.PriceCents,
		})
	}

	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}
