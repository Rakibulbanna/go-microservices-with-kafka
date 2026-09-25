package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/banna/kafka-microservices/pkg/events"
	"github.com/banna/kafka-microservices/services/order-service/internal/model"
	"github.com/banna/kafka-microservices/services/order-service/internal/repository"
)

type OrderService struct {
	orderRepo  *repository.OrderRepository
	outboxRepo *repository.OutboxRepository
	db         *sql.DB
	logger     *slog.Logger
}

func NewOrderService(
	orderRepo *repository.OrderRepository,
	outboxRepo *repository.OutboxRepository,
	db *sql.DB,
	logger *slog.Logger,
) *OrderService {
	return &OrderService{
		orderRepo:  orderRepo,
		outboxRepo: outboxRepo,
		db:         db,
		logger:     logger,
	}
}

func (s *OrderService) CreateOrder(ctx context.Context, req model.CreateOrderRequest) (model.Order, error) {
	if err := req.Validate(); err != nil {
		return model.Order{}, fmt.Errorf("validate request: %w", err)
	}

	order := model.NewOrder(req)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Order{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	itemsJSON, err := json.Marshal(order.Items)
	if err != nil {
		return model.Order{}, fmt.Errorf("marshal items: %w", err)
	}

	orderQuery := `
		INSERT INTO orders (id, customer_id, items_json, total_amount, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err = tx.ExecContext(ctx, orderQuery,
		order.ID,
		order.CustomerID,
		string(itemsJSON),
		order.TotalAmount,
		string(order.Status),
		order.CreatedAt,
		order.UpdatedAt,
	)
	if err != nil {
		return model.Order{}, fmt.Errorf("insert order: %w", err)
	}

	eventItems := make([]events.OrderItem, len(order.Items))
	for i, item := range order.Items {
		eventItems[i] = events.OrderItem{
			ProductID: item.ProductID,
			Quantity:  item.Quantity,
			Price:     item.Price,
		}
	}

	eventData := events.OrderCreatedDataV1{
		OrderID:     order.ID,
		CustomerID:  order.CustomerID,
		Items:       eventItems,
		TotalAmount: order.TotalAmount,
		CreatedAt:   order.CreatedAt,
	}

	envelope := events.NewEnvelope(
		events.EventTypeOrderCreated,
		events.EventVersion1,
		events.ProducerOrderService,
		eventData,
	)

	payload, err := json.Marshal(envelope)
	if err != nil {
		return model.Order{}, fmt.Errorf("marshal envelope: %w", err)
	}

	outboxQuery := `
		INSERT INTO outbox_events (id, aggregate_id, event_type, payload, created_at, processed)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err = tx.ExecContext(ctx, outboxQuery,
		envelope.EventID,
		order.ID,
		envelope.EventType,
		string(payload),
		time.Now().UTC(),
		false,
	)
	if err != nil {
		return model.Order{}, fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return model.Order{}, fmt.Errorf("commit transaction: %w", err)
	}

	s.logger.Info("order created with outbox event",
		slog.String("order_id", order.ID),
		slog.String("event_id", envelope.EventID),
		slog.String("correlation_id", envelope.CorrelationID),
	)

	return order, nil
}

func (s *OrderService) GetOrder(ctx context.Context, id string) (model.Order, error) {
	return s.orderRepo.GetByID(ctx, id)
}
