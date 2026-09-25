package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/banna/kafka-microservices/services/notification-service/internal/model"
)

type NotificationRepository struct {
	db *sql.DB
}

func NewNotificationRepository(db *sql.DB) *NotificationRepository {
	return &NotificationRepository{db: db}
}

func (r *NotificationRepository) Create(ctx context.Context, notification model.Notification) error {
	query := `
		INSERT INTO notifications (id, event_id, order_id, customer_id, type, message, sent_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := r.db.ExecContext(ctx, query,
		notification.ID,
		notification.EventID,
		notification.OrderID,
		notification.CustomerID,
		string(notification.Type),
		notification.Message,
		notification.SentAt,
		notification.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert notification: %w", err)
	}
	return nil
}

func (r *NotificationRepository) ExistsByEventID(ctx context.Context, eventID string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM notifications WHERE event_id = $1)`
	err := r.db.QueryRowContext(ctx, query, eventID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check existence: %w", err)
	}
	return exists, nil
}
