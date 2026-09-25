package kafka

import (
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type ProducerConfig struct {
	Brokers        []string
	RequiredAcks   kafkago.RequiredAcks
	MaxAttempts    int
	BatchSize      int
	BatchTimeout   time.Duration
	Compression    kafkago.Compression
}

func DefaultProducerConfig(brokers []string) ProducerConfig {
	return ProducerConfig{
		Brokers:      brokers,
		RequiredAcks: kafkago.RequireAll,
		MaxAttempts:  10,
		BatchSize:    100,
		BatchTimeout: time.Second,
		Compression:  kafkago.Snappy,
	}
}

func NewWriter(config ProducerConfig) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(config.Brokers...),
		RequiredAcks:           config.RequiredAcks,
		MaxAttempts:            config.MaxAttempts,
		BatchSize:              config.BatchSize,
		BatchTimeout:           config.BatchTimeout,
		Compression:            config.Compression,
		AllowAutoTopicCreation: false,
		Balancer:               &kafkago.Hash{},
	}
}
