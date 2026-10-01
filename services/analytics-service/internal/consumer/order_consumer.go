package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/banna/kafka-microservices/pkg/events"
	kafkapkg "github.com/banna/kafka-microservices/pkg/kafka"
	"github.com/banna/kafka-microservices/services/analytics-service/internal/transformer"
)

type OrderConsumer struct {
	reader    *kafkago.Reader
	writer    *kafkago.Writer
	logger    *slog.Logger
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

			c.logger.Info("message received",
				slog.String("topic", msg.Topic),
				slog.Int("partition", msg.Partition),
				slog.Int64("offset", msg.Offset),
				slog.String("key", string(msg.Key)),
				slog.String("group", c.reader.Config().GroupID),
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
				c.logger.Info("offset committed",
					slog.String("topic", msg.Topic),
					slog.Int("partition", msg.Partition),
					slog.Int64("offset", msg.Offset),
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

	c.logger.Info("processing event",
		slog.String("event_type", envelope.EventType),
		slog.String("event_id", eventID),
		slog.String("correlation_id", correlationID),
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

	c.logger.Info("analytics event published (NO DB - pure Kafka)",
		slog.String("event_type", analyticsEvent.EventType),
		slog.String("event_id", analyticsEvent.EventID),
		slog.String("order_id", analyticsEvent.OrderID),
		slog.String("customer_id", analyticsEvent.CustomerID),
		slog.Float64("total_amount", analyticsEvent.TotalAmount),
		slog.Int("item_count", analyticsEvent.ItemCount),
		slog.String("topic", kafkapkg.TopicAnalytics),
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
