package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/banna/kafka-microservices/pkg/events"
	kafkapkg "github.com/banna/kafka-microservices/pkg/kafka"
	"github.com/banna/kafka-microservices/services/payment-service/internal/config"
	"github.com/banna/kafka-microservices/services/payment-service/internal/service"
)

type OrderConsumer struct {
	reader        *kafkago.Reader
	writer        *kafkago.Writer
	paymentSvc    *service.PaymentService
	logger        *slog.Logger
	cfg           config.Config
}

func NewOrderConsumer(
	reader *kafkago.Reader,
	writer *kafkago.Writer,
	paymentSvc *service.PaymentService,
	logger *slog.Logger,
	cfg config.Config,
) *OrderConsumer {
	return &OrderConsumer{
		reader:     reader,
		writer:     writer,
		paymentSvc: paymentSvc,
		logger:     logger,
		cfg:        cfg,
	}
}

func (c *OrderConsumer) Start(ctx context.Context) {
	c.logger.Info("order consumer started",
		slog.String("group", c.reader.Config().GroupID),
		slog.String("topic", c.reader.Config().Topic),
	)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("order consumer shutting down")
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

			c.logMessageReceived(msg)

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

	eventType := c.getHeader(msg, "event_type")
	eventID := c.getHeader(msg, "event_id")
	correlationID := c.getHeader(msg, "correlation_id")
	retryCount := c.getRetryCount(msg)

	c.logger.Info("processing event",
		slog.String("event_type", eventType),
		slog.String("event_id", eventID),
		slog.String("correlation_id", correlationID),
		slog.Int("retry_count", retryCount),
	)

	switch envelope.EventType {
	case events.EventTypeOrderCreated:
		return c.handleOrderCreated(ctx, envelope, msg, retryCount)
	default:
		c.logger.Warn("unknown event type", slog.String("event_type", envelope.EventType))
		return nil
	}
}

func (c *OrderConsumer) handleOrderCreated(ctx context.Context, envelope events.Envelope, msg kafkago.Message, retryCount int) error {
	dataBytes, err := json.Marshal(envelope.Data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	var orderData events.OrderCreatedDataV1
	if err := json.Unmarshal(dataBytes, &orderData); err != nil {
		return fmt.Errorf("unmarshal order data: %w", err)
	}

	payment, isDuplicate, err := c.paymentSvc.ProcessPayment(ctx, envelope.EventID, orderData.OrderID, orderData.TotalAmount)
	if err != nil {
		return c.handleProcessingError(ctx, envelope, msg, err, retryCount)
	}

	if isDuplicate {
		c.logger.Info("payment already processed (idempotent)",
			slog.String("payment_id", payment.ID),
			slog.String("event_id", envelope.EventID),
		)
		return nil
	}

	var resultEnvelope events.Envelope
	var resultTopic string

	if payment.Status == "COMPLETED" {
		resultData := events.PaymentCompletedData{
			PaymentID:   payment.ID,
			OrderID:     payment.OrderID,
			CustomerID:  orderData.CustomerID,
			Amount:      payment.Amount,
			Method:      payment.Method,
			CompletedAt: payment.ProcessedAt,
		}
		resultEnvelope = events.NewEnvelopeWithCorrelation(
			events.EventTypePaymentCompleted,
			events.EventVersion1,
			events.ProducerPaymentService,
			envelope.CorrelationID,
			envelope.EventID,
			resultData,
		)
		resultTopic = kafkapkg.TopicPayments
	} else {
		resultData := events.PaymentFailedData{
			PaymentID:  payment.ID,
			OrderID:    payment.OrderID,
			CustomerID: orderData.CustomerID,
			Amount:     payment.Amount,
			Reason:     "payment processing failed",
			FailedAt:   payment.ProcessedAt,
		}
		resultEnvelope = events.NewEnvelopeWithCorrelation(
			events.EventTypePaymentFailed,
			events.EventVersion1,
			events.ProducerPaymentService,
			envelope.CorrelationID,
			envelope.EventID,
			resultData,
		)
		resultTopic = kafkapkg.TopicPayments
	}

	if err := kafkapkg.PublishEvent(ctx, c.writer, resultTopic, orderData.OrderID, resultEnvelope); err != nil {
		return fmt.Errorf("publish payment event: %w", err)
	}

	c.logger.Info("payment event published",
		slog.String("event_type", resultEnvelope.EventType),
		slog.String("event_id", resultEnvelope.EventID),
		slog.String("topic", resultTopic),
		slog.String("order_id", orderData.OrderID),
	)

	return nil
}

func (c *OrderConsumer) handleProcessingError(ctx context.Context, envelope events.Envelope, msg kafkago.Message, processingErr error, retryCount int) error {
	c.logger.Error("payment processing error",
		slog.String("error", processingErr.Error()),
		slog.String("event_id", envelope.EventID),
		slog.Int("retry_count", retryCount),
	)

	if retryCount >= c.cfg.MaxRetries {
		c.logger.Warn("max retries exceeded - sending to DLT",
			slog.String("event_id", envelope.EventID),
			slog.Int("retry_count", retryCount),
			slog.Int("max_retries", c.cfg.MaxRetries),
		)
		return c.sendToDLT(ctx, envelope, msg, processingErr, retryCount)
	}

	c.logger.Info("sending to retry topic",
		slog.String("event_id", envelope.EventID),
		slog.Int("retry_count", retryCount+1),
	)
	return c.sendToRetry(ctx, envelope, msg, retryCount+1)
}

func (c *OrderConsumer) sendToRetry(ctx context.Context, envelope events.Envelope, msg kafkago.Message, retryCount int) error {
	return c.writer.WriteMessages(ctx, kafkago.Message{
		Topic: kafkapkg.TopicPaymentsRetry,
		Key:   msg.Key,
		Value: msg.Value,
		Headers: append(msg.Headers, kafkago.Header{
			Key:   "retry_count",
			Value: []byte(fmt.Sprintf("%d", retryCount)),
		}),
	})
}

func (c *OrderConsumer) sendToDLT(ctx context.Context, envelope events.Envelope, msg kafkago.Message, processingErr error, retryCount int) error {
	dltEnvelope := events.Envelope{
		EventID:       envelope.EventID,
		EventType:     envelope.EventType + ".dlt",
		Version:       envelope.Version,
		OccurredAt:    envelope.OccurredAt,
		CorrelationID: envelope.CorrelationID,
		CausationID:   envelope.CausationID,
		Producer:      events.ProducerPaymentService,
		Data: map[string]any{
			"original_topic":  msg.Topic,
			"partition":       msg.Partition,
			"offset":          msg.Offset,
			"event_id":        envelope.EventID,
			"error":           processingErr.Error(),
			"retry_count":     retryCount,
			"original_payload": string(msg.Value),
			"timestamp":       time.Now().UTC(),
		},
	}

	return kafkapkg.PublishEvent(ctx, c.writer, kafkapkg.TopicPaymentsDLT, string(msg.Key), dltEnvelope)
}

func (c *OrderConsumer) logMessageReceived(msg kafkago.Message) {
	c.logger.Info("message received",
		slog.String("topic", msg.Topic),
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
		slog.String("key", string(msg.Key)),
		slog.String("group", c.reader.Config().GroupID),
	)
}

func (c *OrderConsumer) getHeader(msg kafkago.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *OrderConsumer) getRetryCount(msg kafkago.Message) int {
	for _, h := range msg.Headers {
		if h.Key == "retry_count" {
			count, _ := strconv.Atoi(string(h.Value))
			return count
		}
	}
	return 0
}
