package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/banna/kafka-microservices/services/order-service/internal/model"
	"github.com/banna/kafka-microservices/services/order-service/internal/service"
)

type OrderHandler struct {
	service *service.OrderService
	logger  *slog.Logger
}

func NewOrderHandler(service *service.OrderService, logger *slog.Logger) *OrderHandler {
	return &OrderHandler{service: service, logger: logger}
}

func (h *OrderHandler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/orders", h.CreateOrder).Methods("POST")
	r.HandleFunc("/orders/{id}", h.GetOrder).Methods("GET")
	r.HandleFunc("/health", h.Health).Methods("GET")
}

func (h *OrderHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var req model.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	order, err := h.service.CreateOrder(r.Context(), req)
	if err != nil {
		h.logger.Error("failed to create order", slog.String("error", err.Error()))

		var orderErr *model.OrderError
		if errors.As(err, &orderErr) {
			h.writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		h.writeError(w, http.StatusInternalServerError, "failed to create order")
		return
	}

	h.logger.Info("order created via REST",
		slog.String("order_id", order.ID),
		slog.String("customer_id", order.CustomerID),
	)

	h.writeJSON(w, http.StatusCreated, order)
}

func (h *OrderHandler) GetOrder(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	order, err := h.service.GetOrder(r.Context(), id)
	if err != nil {
		if errors.Is(err, model.ErrOrderNotFound) {
			h.writeError(w, http.StatusNotFound, "order not found")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "failed to get order")
		return
	}

	h.writeJSON(w, http.StatusOK, order)
}

func (h *OrderHandler) Health(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func (h *OrderHandler) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *OrderHandler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}
