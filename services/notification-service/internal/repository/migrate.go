package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func Migrate(ctx context.Context, db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS notifications (
			id VARCHAR(36) PRIMARY KEY,
			event_id VARCHAR(255) NOT NULL UNIQUE,
			order_id VARCHAR(255) NOT NULL,
			customer_id VARCHAR(255) NOT NULL,
			type VARCHAR(50) NOT NULL,
			message TEXT NOT NULL,
			sent_at TIMESTAMP NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_notifications_event_id ON notifications (event_id)`,
		`CREATE INDEX IF NOT EXISTS idx_notifications_order_id ON notifications (order_id)`,
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
