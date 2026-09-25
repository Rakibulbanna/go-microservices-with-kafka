package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/banna/kafka-microservices/pkg/events"
	kafkapkg "github.com/banna/kafka-microservices/pkg/kafka"
	"github.com/banna/kafka-microservices/services/order-service/internal/repository"
)

type Publisher struct {
	outboxRepo *repository.OutboxRepository
	writer     *kafkago.Writer
	logger     *slog.Logger
	interval   time.Duration
	batchSize  int
}

func NewPublisher(
	outboxRepo *repository.OutboxRepository,
	writer *kafkago.Writer,
	logger *slog.Logger,
	interval time.Duration,
	batchSize int,
) *Publisher {
	return &Publisher{
		outboxRepo: outboxRepo,
		writer:     writer,
		logger:     logger,
		interval:   interval,
		batchSize:  batchSize,
	}
}

func (p *Publisher) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	p.logger.Info("outbox publisher started",
		slog.Duration("interval", p.interval),
		slog.Int("batch_size", p.batchSize),
	)

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("outbox publisher shutting down")
			return
		case <-ticker.C:
			p.publishPending(ctx)
		}
	}
}

func (p *Publisher) publishPending(ctx context.Context) {
	evts, err := p.outboxRepo.GetUnprocessed(ctx, p.batchSize)
	if err != nil {
		p.logger.Error("failed to get unprocessed outbox events", slog.String("error", err.Error()))
		return
	}

	if len(evts) == 0 {
		return
	}

	p.logger.Info("processing outbox events", slog.Int("count", len(evts)))

	for _, evt := range evts {
		if err := p.publishEvent(ctx, evt); err != nil {
			p.logger.Error("failed to publish outbox event",
				slog.String("event_id", evt.ID),
				slog.String("error", err.Error()),
			)
			continue
		}

		if err := p.outboxRepo.MarkProcessed(ctx, evt.ID); err != nil {
			p.logger.Error("failed to mark outbox event as processed",
				slog.String("event_id", evt.ID),
				slog.String("error", err.Error()),
			)
			continue
		}

		p.logger.Info("outbox event published",
			slog.String("event_id", evt.ID),
			slog.String("event_type", evt.EventType),
			slog.String("aggregate_id", evt.AggregateID),
		)
	}
}

func (p *Publisher) publishEvent(ctx context.Context, evt repository.OutboxEvent) error {
	var envelope events.Envelope
	if err := json.Unmarshal([]byte(evt.Payload), &envelope); err != nil {
		return err
	}

	topic := kafkapkg.TopicOrders

	return kafkapkg.PublishEvent(ctx, p.writer, topic, evt.AggregateID, envelope)
}
