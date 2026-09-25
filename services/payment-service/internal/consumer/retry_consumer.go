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

type RetryConsumer struct {
	reader     *kafkago.Reader
	writer     *kafkago.Writer
	paymentSvc *service.PaymentService
	logger     *slog.Logger
	cfg        config.Config
}

func NewRetryConsumer(
	reader *kafkago.Reader,
	writer *kafkago.Writer,
	paymentSvc *service.PaymentService,
	logger *slog.Logger,
	cfg config.Config,
) *RetryConsumer {
	return &RetryConsumer{
		reader:     reader,
		writer:     writer,
		paymentSvc: paymentSvc,
		logger:     logger,
		cfg:        cfg,
	}
}

func (c *RetryConsumer) Start(ctx context.Context) {
	c.logger.Info("retry consumer started",
		slog.String("group", c.reader.Config().GroupID),
		slog.String("topic", c.reader.Config().Topic),
	)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("retry consumer shutting down")
			return
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.logger.Error("failed to fetch retry message", slog.String("error", err.Error()))
				time.Sleep(time.Second)
				continue
			}

			c.logger.Info("retry message received",
				slog.String("topic", msg.Topic),
				slog.Int("partition", msg.Partition),
				slog.Int64("offset", msg.Offset),
				slog.String("key", string(msg.Key)),
			)

			retryCount := c.getRetryCount(msg)

			if retryCount > c.cfg.MaxRetries {
				c.logger.Warn("retry limit exceeded in retry consumer",
					slog.Int("retry_count", retryCount),
					slog.Int("max_retries", c.cfg.MaxRetries),
				)
				c.reader.CommitMessages(ctx, msg)
				continue
			}

			time.Sleep(time.Duration(retryCount) * 2 * time.Second)

			var envelope events.Envelope
			if err := json.Unmarshal(msg.Value, &envelope); err != nil {
				c.logger.Error("failed to unmarshal retry envelope", slog.String("error", err.Error()))
				c.reader.CommitMessages(ctx, msg)
				continue
			}

			dataBytes, _ := json.Marshal(envelope.Data)
			var orderData events.OrderCreatedDataV1
			json.Unmarshal(dataBytes, &orderData)

			_, _, err = c.paymentSvc.ProcessPayment(ctx, envelope.EventID, orderData.OrderID, orderData.TotalAmount)
			if err != nil {
				c.logger.Error("retry processing failed",
					slog.String("error", err.Error()),
					slog.Int("retry_count", retryCount),
				)

				newRetryCount := retryCount + 1
				if newRetryCount > c.cfg.MaxRetries {
					dltEnvelope := events.Envelope{
						EventID:       envelope.EventID,
						EventType:     envelope.EventType + ".dlt",
						Version:       envelope.Version,
						OccurredAt:    envelope.OccurredAt,
						CorrelationID: envelope.CorrelationID,
						Producer:      events.ProducerPaymentService,
						Data: map[string]any{
							"original_topic":   msg.Topic,
							"partition":        msg.Partition,
							"offset":           msg.Offset,
							"event_id":         envelope.EventID,
							"error":            err.Error(),
							"retry_count":      newRetryCount,
							"original_payload": string(msg.Value),
							"timestamp":        time.Now().UTC(),
						},
					}
					kafkapkg.PublishEvent(ctx, c.writer, kafkapkg.TopicPaymentsDLT, string(msg.Key), dltEnvelope)
				} else {
					c.writer.WriteMessages(ctx, kafkago.Message{
						Topic: kafkapkg.TopicPaymentsRetry,
						Key:   msg.Key,
						Value: msg.Value,
						Headers: []kafkago.Header{
							{Key: "retry_count", Value: []byte(fmt.Sprintf("%d", newRetryCount))},
						},
					})
				}
			}

			c.reader.CommitMessages(ctx, msg)
		}
	}
}

func (c *RetryConsumer) getRetryCount(msg kafkago.Message) int {
	for _, h := range msg.Headers {
		if h.Key == "retry_count" {
			count, _ := strconv.Atoi(string(h.Value))
			return count
		}
	}
	return 0
}
