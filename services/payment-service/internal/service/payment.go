package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/banna/kafka-microservices/services/payment-service/internal/model"
	"github.com/banna/kafka-microservices/services/payment-service/internal/repository"
)

type PaymentService struct {
	repo   *repository.PaymentRepository
	logger *slog.Logger
}

func NewPaymentService(repo *repository.PaymentRepository, logger *slog.Logger) *PaymentService {
	return &PaymentService{
		repo:   repo,
		logger: logger,
	}
}

func (s *PaymentService) ProcessPayment(ctx context.Context, eventID, orderID string, amount float64) (model.Payment, bool, error) {
	exists, err := s.repo.ExistsByEventID(ctx, eventID)
	if err != nil {
		return model.Payment{}, false, fmt.Errorf("check idempotency: %w", err)
	}

	if exists {
		s.logger.Info("duplicate payment event detected - idempotency check",
			slog.String("event_id", eventID),
			slog.String("order_id", orderID),
		)
		payment, err := s.repo.GetByEventID(ctx, eventID)
		if err != nil {
			return model.Payment{}, false, fmt.Errorf("get existing payment: %w", err)
		}
		return payment, true, nil
	}

	payment := model.Payment{
		ID:          uuid.New().String(),
		OrderID:     orderID,
		EventID:     eventID,
		Amount:      amount,
		Method:      "card",
		Status:      model.PaymentStatusPending,
		CreatedAt:   time.Now().UTC(),
	}

	if err := s.repo.Create(ctx, payment); err != nil {
		return model.Payment{}, false, fmt.Errorf("create payment: %w", err)
	}

	success := amount > 0 && amount < 10000

	if success {
		payment.Status = model.PaymentStatusCompleted
		payment.ProcessedAt = time.Now().UTC()
	} else {
		payment.Status = model.PaymentStatusFailed
		payment.ProcessedAt = time.Now().UTC()
	}

	if err := s.repo.UpdateStatus(ctx, payment.ID, payment.Status); err != nil {
		return model.Payment{}, false, fmt.Errorf("update payment status: %w", err)
	}

	s.logger.Info("payment processed",
		slog.String("payment_id", payment.ID),
		slog.String("event_id", eventID),
		slog.String("order_id", orderID),
		slog.String("status", string(payment.Status)),
		slog.Float64("amount", amount),
	)

	return payment, false, nil
}
