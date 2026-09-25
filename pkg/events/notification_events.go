package events

import "time"

type NotificationSentData struct {
	NotificationID string    `json:"notification_id"`
	OrderID        string    `json:"order_id"`
	CustomerID     string    `json:"customer_id"`
	Type           string    `json:"type"`
	Message        string    `json:"message"`
	SentAt         time.Time `json:"sent_at"`
}
