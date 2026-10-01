package transformer

import (
	"time"

	"github.com/banna/kafka-microservices/pkg/events"
)

type AnalyticsEvent struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	OrderID       string    `json:"order_id"`
	CustomerID    string    `json:"customer_id"`
	TotalAmount   float64   `json:"total_amount"`
	ItemCount     int       `json:"item_count"`
	ProcessedAt   time.Time `json:"processed_at"`
}

func TransformOrderCreated(envelope events.Envelope, data events.OrderCreatedDataV1) AnalyticsEvent {
	itemCount := 0
	for _, item := range data.Items {
		itemCount += item.Quantity
	}

	return AnalyticsEvent{
		EventID:     envelope.EventID,
		EventType:   events.EventTypeOrderAnalytics,
		OrderID:     data.OrderID,
		CustomerID:  data.CustomerID,
		TotalAmount: data.TotalAmount,
		ItemCount:   itemCount,
		ProcessedAt: time.Now().UTC(),
	}
}
