package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type OutboxEvent struct {
	ID          string
	AggregateID string
	EventType   string
	Payload     string
	CreatedAt   time.Time
	Processed   bool
}

type OutboxRepository struct {
	db *sql.DB
}

func NewOutboxRepository(db *sql.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

func (r *OutboxRepository) Insert(ctx context.Context, tx *sql.Tx, aggregateID, eventType, payload string) error {
	query := `
		INSERT INTO outbox_events (id, aggregate_id, event_type, payload, created_at, processed)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := tx.ExecContext(ctx, query,
		uuid.New().String(),
		aggregateID,
		eventType,
		payload,
		time.Now().UTC(),
		false,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func (r *OutboxRepository) GetUnprocessed(ctx context.Context, limit int) ([]OutboxEvent, error) {
	query := `
		SELECT id, aggregate_id, event_type, payload, created_at, processed
		FROM outbox_events
		WHERE processed = false
		ORDER BY created_at ASC
		LIMIT $1
	`
	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("query unprocessed: %w", err)
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.EventType, &e.Payload, &e.CreatedAt, &e.Processed); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (r *OutboxRepository) MarkProcessed(ctx context.Context, id string) error {
	query := `UPDATE outbox_events SET processed = true WHERE id = $1`
	_, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("mark processed: %w", err)
	}
	return nil
}
