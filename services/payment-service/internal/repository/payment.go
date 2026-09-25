package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/banna/kafka-microservices/services/payment-service/internal/model"
)

type PaymentRepository struct {
	db *sql.DB
}

func NewPaymentRepository(db *sql.DB) *PaymentRepository {
	return &PaymentRepository{db: db}
}

func (r *PaymentRepository) Create(ctx context.Context, payment model.Payment) error {
	query := `
		INSERT INTO payments (id, order_id, event_id, amount, method, status, processed_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := r.db.ExecContext(ctx, query,
		payment.ID,
		payment.OrderID,
		payment.EventID,
		payment.Amount,
		payment.Method,
		string(payment.Status),
		payment.ProcessedAt,
		payment.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert payment: %w", err)
	}
	return nil
}

func (r *PaymentRepository) GetByEventID(ctx context.Context, eventID string) (model.Payment, error) {
	var payment model.Payment
	query := `
		SELECT id, order_id, event_id, amount, method, status, processed_at, created_at
		FROM payments WHERE event_id = $1
	`
	err := r.db.QueryRowContext(ctx, query, eventID).Scan(
		&payment.ID,
		&payment.OrderID,
		&payment.EventID,
		&payment.Amount,
		&payment.Method,
		&payment.Status,
		&payment.ProcessedAt,
		&payment.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return model.Payment{}, nil
	}
	if err != nil {
		return model.Payment{}, fmt.Errorf("query payment: %w", err)
	}
	return payment, nil
}

func (r *PaymentRepository) UpdateStatus(ctx context.Context, id string, status model.PaymentStatus) error {
	query := `UPDATE payments SET status = $1, processed_at = $2 WHERE id = $3`
	_, err := r.db.ExecContext(ctx, query, string(status), time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

func (r *PaymentRepository) ExistsByEventID(ctx context.Context, eventID string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM payments WHERE event_id = $1)`
	err := r.db.QueryRowContext(ctx, query, eventID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check existence: %w", err)
	}
	return exists, nil
}
