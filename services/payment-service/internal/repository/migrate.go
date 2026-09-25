package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func Migrate(ctx context.Context, db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS payments (
			id VARCHAR(36) PRIMARY KEY,
			order_id VARCHAR(255) NOT NULL,
			event_id VARCHAR(255) NOT NULL UNIQUE,
			amount DECIMAL(10,2) NOT NULL,
			method VARCHAR(50) NOT NULL DEFAULT 'card',
			status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
			processed_at TIMESTAMP,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_payments_event_id ON payments (event_id)`,
		`CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments (order_id)`,
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
