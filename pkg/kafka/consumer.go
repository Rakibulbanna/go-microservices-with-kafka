package kafka

import (
	"context"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type ConsumerConfig struct {
	Brokers        []string
	GroupID        string
	Topic          string
	MinBytes       int
	MaxBytes       int
	MaxWait        time.Duration
	StartOffset    int64
	RetentionTime  time.Duration
}

func DefaultConsumerConfig(brokers []string, groupID, topic string) ConsumerConfig {
	return ConsumerConfig{
		Brokers:       brokers,
		GroupID:       groupID,
		Topic:         topic,
		MinBytes:      10e3,
		MaxBytes:      10e6,
		MaxWait:       5 * time.Second,
		StartOffset:   kafkago.FirstOffset,
		RetentionTime: time.Hour * 24,
	}
}

func NewReader(config ConsumerConfig) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     config.Brokers,
		GroupID:     config.GroupID,
		Topic:       config.Topic,
		MinBytes:    config.MinBytes,
		MaxBytes:    config.MaxBytes,
		MaxWait:     config.MaxWait,
		StartOffset: config.StartOffset,
		RetentionTime: config.RetentionTime,
	})
}

type MessageHandler func(ctx context.Context, msg kafkago.Message) error

func ConsumeMessages(ctx context.Context, reader *kafkago.Reader, handler MessageHandler, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			logger.Info("consumer shutting down",
				slog.String("group", reader.Config().GroupID),
				slog.String("topic", reader.Config().Topic),
			)
			return
		default:
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Error("failed to fetch message",
					slog.String("error", err.Error()),
					slog.String("group", reader.Config().GroupID),
					slog.String("topic", reader.Config().Topic),
				)
				time.Sleep(time.Second)
				continue
			}

			logger.Info("message received",
				slog.String("topic", msg.Topic),
				slog.Int("partition", msg.Partition),
				slog.Int64("offset", msg.Offset),
				slog.String("key", string(msg.Key)),
				slog.String("group", reader.Config().GroupID),
			)

			if err := handler(ctx, msg); err != nil {
				logger.Error("message processing failed",
					slog.String("error", err.Error()),
					slog.String("topic", msg.Topic),
					slog.Int("partition", msg.Partition),
					slog.Int64("offset", msg.Offset),
					slog.String("key", string(msg.Key)),
				)
				continue
			}

			if err := reader.CommitMessages(ctx, msg); err != nil {
				logger.Error("failed to commit offset",
					slog.String("error", err.Error()),
					slog.Int64("offset", msg.Offset),
				)
			} else {
				logger.Info("offset committed",
					slog.String("topic", msg.Topic),
					slog.Int("partition", msg.Partition),
					slog.Int64("offset", msg.Offset),
				)
			}
		}
	}
}
