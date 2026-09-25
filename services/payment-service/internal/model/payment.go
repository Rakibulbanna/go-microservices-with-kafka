package model

import (
	"time"
)

type PaymentStatus string

const (
	PaymentStatusPending   PaymentStatus = "PENDING"
	PaymentStatusCompleted PaymentStatus = "COMPLETED"
	PaymentStatusFailed    PaymentStatus = "FAILED"
)

type Payment struct {
	ID          string        `json:"id" db:"id"`
	OrderID     string        `json:"order_id" db:"order_id"`
	EventID     string        `json:"event_id" db:"event_id"`
	Amount      float64       `json:"amount" db:"amount"`
	Method      string        `json:"method" db:"method"`
	Status      PaymentStatus `json:"status" db:"status"`
	ProcessedAt time.Time     `json:"processed_at" db:"processed_at"`
	CreatedAt   time.Time     `json:"created_at" db:"created_at"`
}

type ProcessPaymentRequest struct {
	EventID string  `json:"event_id"`
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
	Method  string  `json:"method"`
}
