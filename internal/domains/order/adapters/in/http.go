package in

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	inventoryentities "github.com/fastprodman/consistent-store/internal/domains/inventory/entities"
	"github.com/fastprodman/consistent-store/internal/domains/order/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/order/ports/in"
	portsout "github.com/fastprodman/consistent-store/internal/domains/order/ports/out"
	orderservices "github.com/fastprodman/consistent-store/internal/domains/order/services"
	"github.com/google/uuid"
)

type HTTPHandler struct {
	ordersService portsin.Service
	ordersRepo    portsout.Repository
}

func NewHTTPHandler(
	ordersService portsin.Service,
	ordersRepo portsout.Repository,
) *HTTPHandler {
	return &HTTPHandler{
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

func (h *HTTPHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "invalid json body",
		})
		return
	}

	cmd := portsin.CreateOrderCommand{
		CustomerID: req.CustomerID,
		Items:      make([]portsin.CreateOrderItem, 0, len(req.Items)),
	}

	for _, item := range req.Items {
		cmd.Items = append(cmd.Items, portsin.CreateOrderItem{
			SKU:      item.SKU,
			Quantity: item.Quantity,
		})
	}

	orderID, err := h.ordersService.CreateOrder(r.Context(), cmd)
	if err != nil {
		status := http.StatusInternalServerError

		switch {
		case errors.Is(err, orderservices.ErrEmptyCustomerID),
			errors.Is(err, orderservices.ErrEmptyItems),
			errors.Is(err, inventoryentities.ErrInvalidQuantity):
			status = http.StatusBadRequest

		case errors.Is(err, inventoryentities.ErrInsufficientInventory):
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

func (h *HTTPHandler) GetOrder(w http.ResponseWriter, r *http.Request) {
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

	writeJSON(w, http.StatusOK, newOrderResponse(order, items))
}

func newOrderResponse(order entities.Order, items []entities.OrderItem) orderResponse {
	resp := orderResponse{
		ID:         order.ID().String(),
		CustomerID: order.CustomerID(),
		Status:     string(order.Status()),
		TotalCents: order.TotalCents(),
		Items:      make([]orderItemResponse, 0, len(items)),
	}

	for _, item := range items {
		resp.Items = append(resp.Items, orderItemResponse{
			SKU:        item.SKU(),
			Quantity:   item.Quantity(),
			PriceCents: item.PriceCents(),
		})
	}

	return resp
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}
