package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/banna/kafka-microservices/services/order-service/internal/model"
)

type OrderRepository struct {
	db *sql.DB
}

func NewOrderRepository(db *sql.DB) *OrderRepository {
	return &OrderRepository{db: db}
}

func (r *OrderRepository) Create(ctx context.Context, order model.Order) error {
	itemsJSON, err := json.Marshal(order.Items)
	if err != nil {
		return fmt.Errorf("marshal items: %w", err)
	}

	query := `
		INSERT INTO orders (id, customer_id, items_json, total_amount, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err = r.db.ExecContext(ctx, query,
		order.ID,
		order.CustomerID,
		string(itemsJSON),
		order.TotalAmount,
		string(order.Status),
		order.CreatedAt,
		order.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}
	return nil
}

func (r *OrderRepository) GetByID(ctx context.Context, id string) (model.Order, error) {
	var order model.Order
	var itemsJSON string

	query := `
		SELECT id, customer_id, items_json, total_amount, status, created_at, updated_at
		FROM orders WHERE id = $1
	`
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&order.ID,
		&order.CustomerID,
		&itemsJSON,
		&order.TotalAmount,
		&order.Status,
		&order.CreatedAt,
		&order.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return model.Order{}, model.ErrOrderNotFound
	}
	if err != nil {
		return model.Order{}, fmt.Errorf("query order: %w", err)
	}

	if err := json.Unmarshal([]byte(itemsJSON), &order.Items); err != nil {
		return model.Order{}, fmt.Errorf("unmarshal items: %w", err)
	}

	return order, nil
}

func (r *OrderRepository) UpdateStatus(ctx context.Context, id string, status model.OrderStatus) error {
	query := `UPDATE orders SET status = $1, updated_at = $2 WHERE id = $3`
	result, err := r.db.ExecContext(ctx, query, string(status), time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return model.ErrOrderNotFound
	}
	return nil
}
