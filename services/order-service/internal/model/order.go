package model

import (
	"time"

	"github.com/google/uuid"
)

type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "PENDING"
	OrderStatusConfirmed OrderStatus = "CONFIRMED"
	OrderStatusCancelled OrderStatus = "CANCELLED"
	OrderStatusFailed    OrderStatus = "FAILED"
)

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type Order struct {
	ID          string      `json:"id" db:"id"`
	CustomerID  string      `json:"customer_id" db:"customer_id"`
	Items       []OrderItem `json:"items" db:"-"`
	ItemsJSON   string      `json:"-" db:"items_json"`
	TotalAmount float64     `json:"total_amount" db:"total_amount"`
	Status      OrderStatus `json:"status" db:"status"`
	CreatedAt   time.Time   `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at" db:"updated_at"`
}

type CreateOrderRequest struct {
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
}

func (r CreateOrderRequest) Validate() error {
	if r.CustomerID == "" {
		return ErrCustomerIDRequired
	}
	if len(r.Items) == 0 {
		return ErrItemsRequired
	}
	for _, item := range r.Items {
		if item.ProductID == "" {
			return ErrProductIDRequired
		}
		if item.Quantity <= 0 {
			return ErrInvalidQuantity
		}
		if item.Price < 0 {
			return ErrInvalidPrice
		}
	}
	return nil
}

func (r CreateOrderRequest) TotalAmount() float64 {
	var total float64
	for _, item := range r.Items {
		total += float64(item.Quantity) * item.Price
	}
	return total
}

func NewOrder(req CreateOrderRequest) Order {
	return Order{
		ID:          uuid.New().String(),
		CustomerID:  req.CustomerID,
		Items:       req.Items,
		TotalAmount: req.TotalAmount(),
		Status:      OrderStatusPending,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
}

type OrderError struct {
	msg string
}

func (e *OrderError) Error() string { return e.msg }

var (
	ErrCustomerIDRequired = &OrderError{"customer_id is required"}
	ErrItemsRequired      = &OrderError{"items are required"}
	ErrProductIDRequired  = &OrderError{"product_id is required"}
	ErrInvalidQuantity    = &OrderError{"quantity must be greater than 0"}
	ErrInvalidPrice       = &OrderError{"price must not be negative"}
	ErrOrderNotFound      = &OrderError{"order not found"}
)
