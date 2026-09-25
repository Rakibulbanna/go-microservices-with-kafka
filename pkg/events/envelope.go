package events

import (
	"time"

	"github.com/google/uuid"
)

type Envelope struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Version       int       `json:"version"`
	OccurredAt    time.Time `json:"occurred_at"`
	CorrelationID string    `json:"correlation_id"`
	CausationID   string    `json:"causation_id"`
	Producer      string    `json:"producer"`
	Data          any       `json:"data"`
}

func NewEnvelope(eventType string, version int, producer string, data any) Envelope {
	return Envelope{
		EventID:       uuid.New().String(),
		EventType:     eventType,
		Version:       version,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: uuid.New().String(),
		CausationID:   "",
		Producer:      producer,
		Data:          data,
	}
}

func NewEnvelopeWithCorrelation(eventType string, version int, producer string, correlationID string, causationID string, data any) Envelope {
	return Envelope{
		EventID:       uuid.New().String(),
		EventType:     eventType,
		Version:       version,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: correlationID,
		CausationID:   causationID,
		Producer:      producer,
		Data:          data,
	}
}
