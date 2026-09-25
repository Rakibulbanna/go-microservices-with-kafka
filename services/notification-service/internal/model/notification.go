package model

import "time"

type NotificationType string

const (
	NotificationTypeOrderCreated    NotificationType = "ORDER_CREATED"
	NotificationTypePaymentCompleted NotificationType = "PAYMENT_COMPLETED"
	NotificationTypePaymentFailed   NotificationType = "PAYMENT_FAILED"
)

type Notification struct {
	ID          string           `json:"id" db:"id"`
	EventID     string           `json:"event_id" db:"event_id"`
	OrderID     string           `json:"order_id" db:"order_id"`
	CustomerID  string           `json:"customer_id" db:"customer_id"`
	Type        NotificationType `json:"type" db:"type"`
	Message     string           `json:"message" db:"message"`
	SentAt      time.Time        `json:"sent_at" db:"sent_at"`
	CreatedAt   time.Time        `json:"created_at" db:"created_at"`
}
