package in

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/fastprodman/consistent-store/internal/domains/customer/entities"
	portsin "github.com/fastprodman/consistent-store/internal/domains/customer/ports/in"
	customerservices "github.com/fastprodman/consistent-store/internal/domains/customer/services"
)

type HTTPHandler struct {
	customers portsin.Service
}

func NewHTTPHandler(customers portsin.Service) *HTTPHandler {
	return &HTTPHandler{customers: customers}
}

type createCustomerRequest struct {
	CustomerID string `json:"customer_id"`
}

type createCustomerResponse struct {
	CustomerID string `json:"customer_id"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (h *HTTPHandler) CreateCustomer(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeCreateCustomerRequest(w, r)
	if !ok {
		return
	}

	customerID, err := h.customers.CreateCustomer(r.Context(), portsin.CreateCustomerCommand{
		CustomerID: req.CustomerID,
	})
	if err != nil {
		status := http.StatusInternalServerError

		switch {
		case errors.Is(err, entities.ErrEmptyCustomerID):
			status = http.StatusBadRequest
		case errors.Is(err, customerservices.ErrCustomerAlreadyExists):
			status = http.StatusConflict
		}

		writeJSON(w, status, errorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, createCustomerResponse{
		CustomerID: customerID,
	})
}

func decodeCreateCustomerRequest(w http.ResponseWriter, r *http.Request) (createCustomerRequest, bool) {
	var req createCustomerRequest

	if r.Body == nil {
		return req, true
	}

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		if errors.Is(err, io.EOF) {
			return req, true
		}

		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid json body"})
		return createCustomerRequest{}, false
	}

	return req, true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}
