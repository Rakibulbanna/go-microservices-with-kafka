package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func Migrate(ctx context.Context, db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS orders (
			id VARCHAR(36) PRIMARY KEY,
			customer_id VARCHAR(255) NOT NULL,
			items_json TEXT NOT NULL,
			total_amount DECIMAL(10,2) NOT NULL,
			status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS outbox_events (
			id VARCHAR(36) PRIMARY KEY,
			aggregate_id VARCHAR(255) NOT NULL,
			event_type VARCHAR(255) NOT NULL,
			payload TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			processed BOOLEAN NOT NULL DEFAULT FALSE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_outbox_events_unprocessed ON outbox_events (processed, created_at)`,
	}

	for _, q := range queries {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if _, err := db.ExecContext(ctx, q); err != nil {
			cancel()
			return fmt.Errorf("execute migration: %w", err)
		}
		cancel()
	}
	return nil
}
