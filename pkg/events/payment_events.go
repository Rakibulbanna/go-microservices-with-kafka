package events

import "time"

type PaymentCompletedData struct {
	PaymentID   string    `json:"payment_id"`
	OrderID     string    `json:"order_id"`
	CustomerID  string    `json:"customer_id"`
	Amount      float64   `json:"amount"`
	Method      string    `json:"method"`
	CompletedAt time.Time `json:"completed_at"`
}

type PaymentFailedData struct {
	PaymentID  string    `json:"payment_id"`
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Amount     float64   `json:"amount"`
	Reason     string    `json:"reason"`
	FailedAt   time.Time `json:"failed_at"`
}
