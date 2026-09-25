package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/banna/kafka-microservices/services/notification-service/internal/model"
	"github.com/banna/kafka-microservices/services/notification-service/internal/repository"
)

type NotificationService struct {
	repo   *repository.NotificationRepository
	logger *slog.Logger
}

func NewNotificationService(repo *repository.NotificationRepository, logger *slog.Logger) *NotificationService {
	return &NotificationService{
		repo:   repo,
		logger: logger,
	}
}

func (s *NotificationService) SendOrderCreatedNotification(ctx context.Context, eventID, orderID, customerID string) error {
	exists, err := s.repo.ExistsByEventID(ctx, eventID)
	if err != nil {
		return fmt.Errorf("check idempotency: %w", err)
	}
	if exists {
		s.logger.Info("duplicate notification event detected - idempotency check",
			slog.String("event_id", eventID),
		)
		return nil
	}

	notification := model.Notification{
		ID:         uuid.New().String(),
		EventID:    eventID,
		OrderID:    orderID,
		CustomerID: customerID,
		Type:       model.NotificationTypeOrderCreated,
		Message:    fmt.Sprintf("Your order %s has been created successfully.", orderID),
		SentAt:     time.Now().UTC(),
		CreatedAt:  time.Now().UTC(),
	}

	if err := s.repo.Create(ctx, notification); err != nil {
		return fmt.Errorf("create notification: %w", err)
	}

	s.logger.Info("notification sent (simulated)",
		slog.String("notification_id", notification.ID),
		slog.String("event_id", eventID),
		slog.String("order_id", orderID),
		slog.String("type", string(notification.Type)),
		slog.String("message", notification.Message),
	)

	return nil
}

func (s *NotificationService) SendPaymentCompletedNotification(ctx context.Context, eventID, orderID, customerID string, amount float64) error {
	exists, err := s.repo.ExistsByEventID(ctx, eventID)
	if err != nil {
		return fmt.Errorf("check idempotency: %w", err)
	}
	if exists {
		s.logger.Info("duplicate notification event detected - idempotency check",
			slog.String("event_id", eventID),
		)
		return nil
	}

	notification := model.Notification{
		ID:         uuid.New().String(),
		EventID:    eventID,
		OrderID:    orderID,
		CustomerID: customerID,
		Type:       model.NotificationTypePaymentCompleted,
		Message:    fmt.Sprintf("Payment of %.2f for order %s has been completed.", amount, orderID),
		SentAt:     time.Now().UTC(),
		CreatedAt:  time.Now().UTC(),
	}

	if err := s.repo.Create(ctx, notification); err != nil {
		return fmt.Errorf("create notification: %w", err)
	}

	s.logger.Info("notification sent (simulated)",
		slog.String("notification_id", notification.ID),
		slog.String("event_id", eventID),
		slog.String("order_id", orderID),
		slog.String("type", string(notification.Type)),
		slog.String("message", notification.Message),
	)

	return nil
}

func (s *NotificationService) SendPaymentFailedNotification(ctx context.Context, eventID, orderID, customerID string, reason string) error {
	exists, err := s.repo.ExistsByEventID(ctx, eventID)
	if err != nil {
		return fmt.Errorf("check idempotency: %w", err)
	}
	if exists {
		s.logger.Info("duplicate notification event detected - idempotency check",
			slog.String("event_id", eventID),
		)
		return nil
	}

	notification := model.Notification{
		ID:         uuid.New().String(),
		EventID:    eventID,
		OrderID:    orderID,
		CustomerID: customerID,
		Type:       model.NotificationTypePaymentFailed,
		Message:    fmt.Sprintf("Payment for order %s failed: %s", orderID, reason),
		SentAt:     time.Now().UTC(),
		CreatedAt:  time.Now().UTC(),
	}

	if err := s.repo.Create(ctx, notification); err != nil {
		return fmt.Errorf("create notification: %w", err)
	}

	s.logger.Info("notification sent (simulated)",
		slog.String("notification_id", notification.ID),
		slog.String("event_id", eventID),
		slog.String("order_id", orderID),
		slog.String("type", string(notification.Type)),
		slog.String("message", notification.Message),
	)

	return nil
}
