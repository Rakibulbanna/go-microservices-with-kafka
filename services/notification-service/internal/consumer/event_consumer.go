package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/banna/kafka-microservices/pkg/events"
	"github.com/banna/kafka-microservices/pkg/observability"
	"github.com/banna/kafka-microservices/services/notification-service/internal/config"
	"github.com/banna/kafka-microservices/services/notification-service/internal/service"
)

type EventConsumer struct {
	reader          *kafkago.Reader
	notificationSvc *service.NotificationService
	logger          *slog.Logger
	cfg             config.Config
}

func NewEventConsumer(
	reader *kafkago.Reader,
	notificationSvc *service.NotificationService,
	logger *slog.Logger,
	cfg config.Config,
) *EventConsumer {
	return &EventConsumer{
		reader:          reader,
		notificationSvc: notificationSvc,
		logger:          logger,
		cfg:             cfg,
	}
}

func (c *EventConsumer) Start(ctx context.Context) {
	c.logger.Info("event consumer started",
		slog.String("group", c.reader.Config().GroupID),
		slog.String("topic", c.reader.Config().Topic),
	)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("event consumer shutting down")
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

		observability.PrintStep("CONSUME", "notification-service", "message_received",
			"topic", msg.Topic,
			"partition", fmt.Sprintf("%d", msg.Partition),
			"offset", fmt.Sprintf("%d", msg.Offset),
			"key", string(msg.Key),
			"group", c.reader.Config().GroupID,
			"flow", msg.Topic+" → notification-service",
		)

			if c.cfg.SlowConsumer {
				c.logger.Info("slow consumer mode - simulating delay",
					slog.Duration("delay", c.cfg.SlowConsumerDelay),
				)
				time.Sleep(c.cfg.SlowConsumerDelay)
			}

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
			observability.PrintStep("CONSUME", "notification-service", "offset_committed",
				"topic", msg.Topic,
				"partition", fmt.Sprintf("%d", msg.Partition),
				"offset", fmt.Sprintf("%d", msg.Offset),
				"status", "SUCCESS",
			)
		}
		}
	}
}

func (c *EventConsumer) handleMessage(ctx context.Context, msg kafkago.Message) error {
	var envelope events.Envelope
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		c.logger.Error("failed to unmarshal envelope", slog.String("error", err.Error()))
		return err
	}

	eventID := c.getHeader(msg, "event_id")
	correlationID := c.getHeader(msg, "correlation_id")

	observability.PrintStep("PROCESS", "notification-service", "processing_event",
		"event_type", envelope.EventType,
		"event_id", eventID,
		"correlation_id", correlationID,
	)

	switch envelope.EventType {
	case events.EventTypeOrderCreated:
		return c.handleOrderCreated(ctx, envelope)
	case events.EventTypePaymentCompleted:
		return c.handlePaymentCompleted(ctx, envelope)
	case events.EventTypePaymentFailed:
		return c.handlePaymentFailed(ctx, envelope)
	default:
		c.logger.Warn("unknown event type", slog.String("event_type", envelope.EventType))
		return nil
	}
}

func (c *EventConsumer) handleOrderCreated(ctx context.Context, envelope events.Envelope) error {
	dataBytes, err := json.Marshal(envelope.Data)
	if err != nil {
		return err
	}

	var orderData events.OrderCreatedDataV1
	if err := json.Unmarshal(dataBytes, &orderData); err != nil {
		return err
	}

	return c.notificationSvc.SendOrderCreatedNotification(ctx, envelope.EventID, orderData.OrderID, orderData.CustomerID)
}

func (c *EventConsumer) handlePaymentCompleted(ctx context.Context, envelope events.Envelope) error {
	dataBytes, err := json.Marshal(envelope.Data)
	if err != nil {
		return err
	}

	var paymentData events.PaymentCompletedData
	if err := json.Unmarshal(dataBytes, &paymentData); err != nil {
		return err
	}

	return c.notificationSvc.SendPaymentCompletedNotification(ctx, envelope.EventID, paymentData.OrderID, paymentData.CustomerID, paymentData.Amount)
}

func (c *EventConsumer) handlePaymentFailed(ctx context.Context, envelope events.Envelope) error {
	dataBytes, err := json.Marshal(envelope.Data)
	if err != nil {
		return err
	}

	var paymentData events.PaymentFailedData
	if err := json.Unmarshal(dataBytes, &paymentData); err != nil {
		return err
	}

	return c.notificationSvc.SendPaymentFailedNotification(ctx, envelope.EventID, paymentData.OrderID, paymentData.CustomerID, paymentData.Reason)
}

func (c *EventConsumer) getHeader(msg kafkago.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
