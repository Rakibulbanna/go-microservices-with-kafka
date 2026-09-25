package events_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/banna/kafka-microservices/pkg/events"
)

func TestNewEnvelope(t *testing.T) {
	data := events.OrderCreatedDataV1{
		OrderID:     "order-123",
		CustomerID:  "customer-1",
		TotalAmount: 200.00,
		CreatedAt:   time.Now().UTC(),
	}

	envelope := events.NewEnvelope(
		events.EventTypeOrderCreated,
		events.EventVersion1,
		events.ProducerOrderService,
		data,
	)

	if envelope.EventID == "" {
		t.Error("EventID should not be empty")
	}
	if envelope.EventType != events.EventTypeOrderCreated {
		t.Errorf("expected event type %s, got %s", events.EventTypeOrderCreated, envelope.EventType)
	}
	if envelope.Version != events.EventVersion1 {
		t.Errorf("expected version %d, got %d", events.EventVersion1, envelope.Version)
	}
	if envelope.Producer != events.ProducerOrderService {
		t.Errorf("expected producer %s, got %s", events.ProducerOrderService, envelope.Producer)
	}
	if envelope.CorrelationID == "" {
		t.Error("CorrelationID should not be empty")
	}
}

func TestEnvelopeSerialization(t *testing.T) {
	data := events.OrderCreatedDataV1{
		OrderID:     "order-123",
		CustomerID:  "customer-1",
		TotalAmount: 200.00,
		CreatedAt:   time.Now().UTC().Truncate(time.Millisecond),
	}

	envelope := events.NewEnvelope(
		events.EventTypeOrderCreated,
		events.EventVersion1,
		events.ProducerOrderService,
		data,
	)

	bytes, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("failed to marshal envelope: %v", err)
	}

	var deserialized events.Envelope
	if err := json.Unmarshal(bytes, &deserialized); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}

	if deserialized.EventID != envelope.EventID {
		t.Errorf("EventID mismatch: expected %s, got %s", envelope.EventID, deserialized.EventID)
	}
	if deserialized.EventType != envelope.EventType {
		t.Errorf("EventType mismatch: expected %s, got %s", envelope.EventType, deserialized.EventType)
	}
	if deserialized.Version != envelope.Version {
		t.Errorf("Version mismatch: expected %d, got %d", envelope.Version, deserialized.Version)
	}
	if deserialized.CorrelationID != envelope.CorrelationID {
		t.Errorf("CorrelationID mismatch: expected %s, got %s", envelope.CorrelationID, deserialized.CorrelationID)
	}
}

func TestOrderCreatedDataV1Serialization(t *testing.T) {
	data := events.OrderCreatedDataV1{
		OrderID:    "order-123",
		CustomerID: "customer-1",
		Items: []events.OrderItem{
			{ProductID: "product-1", Quantity: 2, Price: 100.00},
			{ProductID: "product-2", Quantity: 1, Price: 50.00},
		},
		TotalAmount: 250.00,
		CreatedAt:   time.Now().UTC().Truncate(time.Millisecond),
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var deserialized events.OrderCreatedDataV1
	if err := json.Unmarshal(bytes, &deserialized); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if deserialized.OrderID != data.OrderID {
		t.Errorf("OrderID mismatch: expected %s, got %s", data.OrderID, deserialized.OrderID)
	}
	if deserialized.CustomerID != data.CustomerID {
		t.Errorf("CustomerID mismatch: expected %s, got %s", data.CustomerID, deserialized.CustomerID)
	}
	if len(deserialized.Items) != len(data.Items) {
		t.Errorf("Items length mismatch: expected %d, got %d", len(data.Items), len(deserialized.Items))
	}
	if deserialized.TotalAmount != data.TotalAmount {
		t.Errorf("TotalAmount mismatch: expected %f, got %f", data.TotalAmount, deserialized.TotalAmount)
	}
}

func TestOrderCreatedDataV2BackwardCompatibility(t *testing.T) {
	v1Data := events.OrderCreatedDataV1{
		OrderID:     "order-123",
		CustomerID:  "customer-1",
		TotalAmount: 200.00,
		CreatedAt:   time.Now().UTC().Truncate(time.Millisecond),
	}

	v1Bytes, err := json.Marshal(v1Data)
	if err != nil {
		t.Fatalf("failed to marshal V1: %v", err)
	}

	var v2Data events.OrderCreatedDataV2
	if err := json.Unmarshal(v1Bytes, &v2Data); err != nil {
		t.Fatalf("V2 consumer should be able to read V1 data: %v", err)
	}

	if v2Data.OrderID != v1Data.OrderID {
		t.Errorf("OrderID mismatch after V1→V2: expected %s, got %s", v1Data.OrderID, v2Data.OrderID)
	}
	if v2Data.CustomerEmail != "" {
		t.Error("V2 CustomerEmail should be empty when reading V1 data")
	}
	if v2Data.Currency != "" {
		t.Error("V2 Currency should be empty when reading V1 data")
	}
}

func TestPaymentCompletedDataSerialization(t *testing.T) {
	data := events.PaymentCompletedData{
		PaymentID:   "payment-123",
		OrderID:     "order-123",
		Amount:      200.00,
		Method:      "card",
		CompletedAt: time.Now().UTC().Truncate(time.Millisecond),
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var deserialized events.PaymentCompletedData
	if err := json.Unmarshal(bytes, &deserialized); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if deserialized.PaymentID != data.PaymentID {
		t.Errorf("PaymentID mismatch: expected %s, got %s", data.PaymentID, deserialized.PaymentID)
	}
	if deserialized.Amount != data.Amount {
		t.Errorf("Amount mismatch: expected %f, got %f", data.Amount, deserialized.Amount)
	}
}

func TestPaymentFailedDataSerialization(t *testing.T) {
	data := events.PaymentFailedData{
		PaymentID: "payment-123",
		OrderID:   "order-123",
		Amount:    200.00,
		Reason:    "insufficient funds",
		FailedAt:  time.Now().UTC().Truncate(time.Millisecond),
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var deserialized events.PaymentFailedData
	if err := json.Unmarshal(bytes, &deserialized); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if deserialized.Reason != data.Reason {
		t.Errorf("Reason mismatch: expected %s, got %s", data.Reason, deserialized.Reason)
	}
}

func TestNewEnvelopeWithCorrelation(t *testing.T) {
	data := events.PaymentCompletedData{
		PaymentID: "payment-123",
		OrderID:   "order-123",
		Amount:    200.00,
	}

	envelope := events.NewEnvelopeWithCorrelation(
		events.EventTypePaymentCompleted,
		events.EventVersion1,
		events.ProducerPaymentService,
		"correlation-abc",
		"causation-xyz",
		data,
	)

	if envelope.CorrelationID != "correlation-abc" {
		t.Errorf("expected correlation_id correlation-abc, got %s", envelope.CorrelationID)
	}
	if envelope.CausationID != "causation-xyz" {
		t.Errorf("expected causation_id causation-xyz, got %s", envelope.CausationID)
	}
}
