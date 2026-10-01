package config

import (
	"os"
	"strings"
)

type Config struct {
	Port         string
	KafkaBrokers []string
	LogLevel     string
}

func Load() Config {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9093"
	}

	return Config{
		Port:         getEnv("ANALYTICS_SERVICE_PORT", "8084"),
		KafkaBrokers: strings.Split(brokers, ","),
		LogLevel:     getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
