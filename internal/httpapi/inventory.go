package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/fastprodman/consistent-store/internal/inventory"
)

type InventoryHandler struct {
	inventory *inventory.Repository
}

func NewInventoryHandler(inventory *inventory.Repository) *InventoryHandler {
	return &InventoryHandler{inventory: inventory}
}

type inventoryResponse struct {
	SKU               string `json:"sku"`
	AvailableQuantity int    `json:"available_quantity"`
	PriceCents        int    `json:"price_cents"`
}

func (h *InventoryHandler) GetInventory(w http.ResponseWriter, r *http.Request) {
	sku := strings.TrimPrefix(r.URL.Path, "/inventory/")
	if sku == "" || sku == r.URL.Path {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "inventory item not found"})
		return
	}

	item, err := h.inventory.Get(r.Context(), sku)
	if err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "inventory item not found"})
			return
		}

		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal server error"})
		return
	}

	writeJSON(w, http.StatusOK, inventoryResponse{
		SKU:               item.SKU,
		AvailableQuantity: item.AvailableQuantity,
		PriceCents:        item.PriceCents,
	})
}
