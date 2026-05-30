package in

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/fastprodman/consistent-store/internal/domains/inventory/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/inventory/ports/in"
)

type HTTPHandler struct {
	inventory portsin.QueryService
}

func NewHTTPHandler(inventory portsin.QueryService) *HTTPHandler {
	return &HTTPHandler{inventory: inventory}
}

type inventoryResponse struct {
	SKU               string `json:"sku"`
	AvailableQuantity int    `json:"available_quantity"`
	PriceCents        int    `json:"price_cents"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (h *HTTPHandler) GetInventory(w http.ResponseWriter, r *http.Request) {
	sku := strings.TrimPrefix(r.URL.Path, "/inventory/")
	if sku == "" || sku == r.URL.Path {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "inventory item not found"})
		return
	}

	item, err := h.inventory.GetInventory(r.Context(), sku)
	if err != nil {
		if errors.Is(err, entities.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "inventory item not found"})
			return
		}

		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal server error"})
		return
	}

	writeJSON(w, http.StatusOK, inventoryResponse{
		SKU:               item.SKU(),
		AvailableQuantity: item.AvailableQuantity(),
		PriceCents:        item.PriceCents(),
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}
