package kafka

import (
	"context"
	"encoding/json"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/banna/kafka-microservices/pkg/events"
)

func PublishEvent(ctx context.Context, writer *kafkago.Writer, topic string, key string, envelope events.Envelope) error {
	value, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	headers := []kafkago.Header{
		{Key: "event_type", Value: []byte(envelope.EventType)},
		{Key: "correlation_id", Value: []byte(envelope.CorrelationID)},
		{Key: "causation_id", Value: []byte(envelope.CausationID)},
		{Key: "event_id", Value: []byte(envelope.EventID)},
		{Key: "producer", Value: []byte(envelope.Producer)},
		{Key: "version", Value: []byte("1")},
	}

	return writer.WriteMessages(ctx, kafkago.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   value,
		Headers: headers,
		Time:    time.Now().UTC(),
	})
}

func PublishEventWithRetry(ctx context.Context, writer *kafkago.Writer, topic string, key string, envelope events.Envelope, retryCount int) error {
	value, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	headers := []kafkago.Header{
		{Key: "event_type", Value: []byte(envelope.EventType)},
		{Key: "correlation_id", Value: []byte(envelope.CorrelationID)},
		{Key: "causation_id", Value: []byte(envelope.CausationID)},
		{Key: "event_id", Value: []byte(envelope.EventID)},
		{Key: "producer", Value: []byte(envelope.Producer)},
		{Key: "version", Value: []byte("1")},
		{Key: "retry_count", Value: []byte("0")},
		{Key: "original_topic", Value: []byte(topic)},
	}

	return writer.WriteMessages(ctx, kafkago.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   value,
		Headers: headers,
		Time:    time.Now().UTC(),
	})
}
