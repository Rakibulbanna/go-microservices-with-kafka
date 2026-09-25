package events

import "time"

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type OrderCreatedDataV1 struct {
	OrderID    string      `json:"order_id"`
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
	TotalAmount float64    `json:"total_amount"`
	CreatedAt  time.Time   `json:"created_at"`
}

type OrderCreatedDataV2 struct {
	OrderID     string      `json:"order_id"`
	CustomerID  string      `json:"customer_id"`
	CustomerEmail string    `json:"customer_email"`
	Items       []OrderItem `json:"items"`
	TotalAmount float64     `json:"total_amount"`
	Currency    string      `json:"currency"`
	CreatedAt   time.Time   `json:"created_at"`
}

type OrderCancelledData struct {
	OrderID     string    `json:"order_id"`
	CustomerID  string    `json:"customer_id"`
	Reason      string    `json:"reason"`
	CancelledAt time.Time `json:"cancelled_at"`
}
