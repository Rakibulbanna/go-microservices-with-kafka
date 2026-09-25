package model_test

import (
	"testing"

	"github.com/banna/kafka-microservices/services/order-service/internal/model"
)

func TestCreateOrderRequestValidate(t *testing.T) {
	tests := []struct {
		name    string
		req     model.CreateOrderRequest
		wantErr error
	}{
		{
			name: "valid request",
			req: model.CreateOrderRequest{
				CustomerID: "customer-1",
				Items: []model.OrderItem{
					{ProductID: "product-1", Quantity: 2, Price: 100.00},
				},
			},
			wantErr: nil,
		},
		{
			name: "missing customer_id",
			req: model.CreateOrderRequest{
				Items: []model.OrderItem{
					{ProductID: "product-1", Quantity: 1, Price: 100.00},
				},
			},
			wantErr: model.ErrCustomerIDRequired,
		},
		{
			name: "empty items",
			req: model.CreateOrderRequest{
				CustomerID: "customer-1",
				Items:      []model.OrderItem{},
			},
			wantErr: model.ErrItemsRequired,
		},
		{
			name: "missing product_id",
			req: model.CreateOrderRequest{
				CustomerID: "customer-1",
				Items: []model.OrderItem{
					{ProductID: "", Quantity: 1, Price: 100.00},
				},
			},
			wantErr: model.ErrProductIDRequired,
		},
		{
			name: "invalid quantity",
			req: model.CreateOrderRequest{
				CustomerID: "customer-1",
				Items: []model.OrderItem{
					{ProductID: "product-1", Quantity: 0, Price: 100.00},
				},
			},
			wantErr: model.ErrInvalidQuantity,
		},
		{
			name: "negative price",
			req: model.CreateOrderRequest{
				CustomerID: "customer-1",
				Items: []model.OrderItem{
					{ProductID: "product-1", Quantity: 1, Price: -10.00},
				},
			},
			wantErr: model.ErrInvalidPrice,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if err != tt.wantErr {
				t.Errorf("expected error %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestCreateOrderRequestTotalAmount(t *testing.T) {
	req := model.CreateOrderRequest{
		CustomerID: "customer-1",
		Items: []model.OrderItem{
			{ProductID: "product-1", Quantity: 2, Price: 100.00},
			{ProductID: "product-2", Quantity: 3, Price: 50.00},
		},
	}

	expected := 350.00
	actual := req.TotalAmount()

	if actual != expected {
		t.Errorf("expected total %f, got %f", expected, actual)
	}
}

func TestNewOrder(t *testing.T) {
	req := model.CreateOrderRequest{
		CustomerID: "customer-1",
		Items: []model.OrderItem{
			{ProductID: "product-1", Quantity: 2, Price: 100.00},
		},
	}

	order := model.NewOrder(req)

	if order.ID == "" {
		t.Error("order ID should not be empty")
	}
	if order.CustomerID != "customer-1" {
		t.Errorf("expected customer_id customer-1, got %s", order.CustomerID)
	}
	if order.TotalAmount != 200.00 {
		t.Errorf("expected total 200.00, got %f", order.TotalAmount)
	}
	if order.Status != model.OrderStatusPending {
		t.Errorf("expected status PENDING, got %s", order.Status)
	}
	if len(order.Items) != 1 {
		t.Errorf("expected 1 item, got %d", len(order.Items))
	}
}
