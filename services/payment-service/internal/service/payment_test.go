package service_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"log/slog"
	"os"

	"github.com/banna/kafka-microservices/services/payment-service/internal/model"
	"github.com/banna/kafka-microservices/services/payment-service/internal/repository"
	"github.com/banna/kafka-microservices/services/payment-service/internal/service"
)

func setupTestDB(t *testing.T) *sql.DB {
	dsn := "host=localhost port=5433 user=postgres password=postgres dbname=payments_db sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("skipping test: database not available: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Skipf("skipping test: database not reachable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := repository.Migrate(ctx, db); err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}

	t.Cleanup(func() {
		db.Exec("DELETE FROM payments")
		db.Close()
	})

	return db
}

func TestProcessPayment_Idempotency(t *testing.T) {
	db := setupTestDB(t)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	repo := repository.NewPaymentRepository(db)
	svc := service.NewPaymentService(repo, logger)

	ctx := context.Background()
	eventID := "test-event-idempotency"
	orderID := "order-idem-1"
	amount := 100.00

	payment1, isDuplicate1, err := svc.ProcessPayment(ctx, eventID, orderID, amount)
	if err != nil {
		t.Fatalf("first processing failed: %v", err)
	}
	if isDuplicate1 {
		t.Error("first processing should not be duplicate")
	}
	if payment1.Status != model.PaymentStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", payment1.Status)
	}

	payment2, isDuplicate2, err := svc.ProcessPayment(ctx, eventID, orderID, amount)
	if err != nil {
		t.Fatalf("second processing failed: %v", err)
	}
	if !isDuplicate2 {
		t.Error("second processing should be detected as duplicate")
	}
	if payment2.ID != payment1.ID {
		t.Error("duplicate should return same payment")
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM payments WHERE event_id = $1", eventID).Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 payment record, got %d", count)
	}
}

func TestProcessPayment_Success(t *testing.T) {
	db := setupTestDB(t)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	repo := repository.NewPaymentRepository(db)
	svc := service.NewPaymentService(repo, logger)

	ctx := context.Background()
	payment, isDuplicate, err := svc.ProcessPayment(ctx, "event-success", "order-1", 500.00)
	if err != nil {
		t.Fatalf("processing failed: %v", err)
	}
	if isDuplicate {
		t.Error("should not be duplicate")
	}
	if payment.Status != model.PaymentStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", payment.Status)
	}
	if payment.Amount != 500.00 {
		t.Errorf("expected amount 500.00, got %f", payment.Amount)
	}
}

func TestProcessPayment_Failure(t *testing.T) {
	db := setupTestDB(t)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	repo := repository.NewPaymentRepository(db)
	svc := service.NewPaymentService(repo, logger)

	ctx := context.Background()
	payment, isDuplicate, err := svc.ProcessPayment(ctx, "event-fail", "order-2", 15000.00)
	if err != nil {
		t.Fatalf("processing failed: %v", err)
	}
	if isDuplicate {
		t.Error("should not be duplicate")
	}
	if payment.Status != model.PaymentStatusFailed {
		t.Errorf("expected FAILED, got %s", payment.Status)
	}
}
