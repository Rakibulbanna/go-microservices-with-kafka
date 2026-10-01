package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/banna/kafka-microservices/pkg/events"
	kafkapkg "github.com/banna/kafka-microservices/pkg/kafka"
	"github.com/banna/kafka-microservices/pkg/observability"
	"github.com/banna/kafka-microservices/services/analytics-service/internal/transformer"
)

type OrderConsumer struct {
	reader *kafkago.Reader
	writer *kafkago.Writer
	logger *slog.Logger
}

func NewOrderConsumer(
	reader *kafkago.Reader,
	writer *kafkago.Writer,
	logger *slog.Logger,
) *OrderConsumer {
	return &OrderConsumer{
		reader: reader,
		writer: writer,
		logger: logger,
	}
}

func (c *OrderConsumer) Start(ctx context.Context) {
	c.logger.Info("analytics consumer started",
		slog.String("group", c.reader.Config().GroupID),
		slog.String("topic", c.reader.Config().Topic),
		slog.String("note", "NO DATABASE - pure Kafka flow"),
	)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("analytics consumer shutting down")
			return
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.logger.Error("failed to fetch message", slog.String("error", err.Error()))
				time.Sleep(time.Second)
				continue
			}

		observability.PrintStep("CONSUME", "analytics-service", "message_received",
			"topic", msg.Topic,
			"partition", fmt.Sprintf("%d", msg.Partition),
			"offset", fmt.Sprintf("%d", msg.Offset),
			"key", string(msg.Key),
			"group", c.reader.Config().GroupID,
			"flow", "orders.v1 → analytics-service (NO DB)",
		)

			if err := c.handleMessage(ctx, msg); err != nil {
				c.logger.Error("message processing failed",
					slog.String("error", err.Error()),
					slog.String("topic", msg.Topic),
					slog.Int("partition", msg.Partition),
					slog.Int64("offset", msg.Offset),
				)
				continue
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error("failed to commit offset",
					slog.String("error", err.Error()),
					slog.Int64("offset", msg.Offset),
				)
		} else {
			observability.PrintStep("CONSUME", "analytics-service", "offset_committed",
				"topic", msg.Topic,
				"partition", fmt.Sprintf("%d", msg.Partition),
				"offset", fmt.Sprintf("%d", msg.Offset),
				"status", "SUCCESS",
			)
		}
		}
	}
}

func (c *OrderConsumer) handleMessage(ctx context.Context, msg kafkago.Message) error {
	var envelope events.Envelope
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		c.logger.Error("failed to unmarshal envelope", slog.String("error", err.Error()))
		return err
	}

	eventID := c.getHeader(msg, "event_id")
	correlationID := c.getHeader(msg, "correlation_id")

	observability.PrintStep("PROCESS", "analytics-service", "processing_event",
		"event_type", envelope.EventType,
		"event_id", eventID,
		"correlation_id", correlationID,
	)

	switch envelope.EventType {
	case events.EventTypeOrderCreated:
		return c.handleOrderCreated(ctx, envelope, msg)
	default:
		c.logger.Warn("unknown event type - skipping",
			slog.String("event_type", envelope.EventType),
		)
		return nil
	}
}

func (c *OrderConsumer) handleOrderCreated(ctx context.Context, envelope events.Envelope, msg kafkago.Message) error {
	dataBytes, err := json.Marshal(envelope.Data)
	if err != nil {
		return err
	}

	var orderData events.OrderCreatedDataV1
	if err := json.Unmarshal(dataBytes, &orderData); err != nil {
		return err
	}

	analyticsEvent := transformer.TransformOrderCreated(envelope, orderData)

	analyticsEnvelope := events.NewEnvelopeWithCorrelation(
		analyticsEvent.EventType,
		events.EventVersion1,
		events.ProducerAnalyticsService,
		envelope.CorrelationID,
		envelope.EventID,
		analyticsEvent,
	)

	if err := kafkapkg.PublishEvent(ctx, c.writer, kafkapkg.TopicAnalytics, orderData.OrderID, analyticsEnvelope); err != nil {
		c.logger.Error("failed to publish analytics event",
			slog.String("error", err.Error()),
			slog.String("order_id", orderData.OrderID),
		)
		return err
	}

	observability.PrintStep("PRODUCE", "analytics-service", "analytics_event_published",
		"event_type", analyticsEvent.EventType,
		"event_id", analyticsEvent.EventID,
		"order_id", analyticsEvent.OrderID,
		"customer_id", analyticsEvent.CustomerID,
		"total_amount", fmt.Sprintf("%.2f", analyticsEvent.TotalAmount),
		"item_count", fmt.Sprintf("%d", analyticsEvent.ItemCount),
		"topic", kafkapkg.TopicAnalytics,
		"note", "NO DATABASE - pure Kafka flow",
		"flow", "analytics-service → analytics.v1",
	)

	return nil
}

func (c *OrderConsumer) getHeader(msg kafkago.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
